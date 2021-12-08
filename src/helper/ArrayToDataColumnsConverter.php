<?php
namespace codename\parquet\helper;

use Exception;

use codename\parquet\data\Field;
use codename\parquet\data\Schema;
use codename\parquet\data\MapField;
use codename\parquet\data\DataField;
use codename\parquet\data\ListField;

use codename\parquet\data\DataColumn;
use codename\parquet\data\SchemaType;
use codename\parquet\data\StructField;

/**
 * Convert PHP array (row-style) data
 * to parquet (given a fixed schema)
 */
class ArrayToDataColumnsConverter
{
  /**
   * [protected description]
   * @var Schema
   */
  protected $schema;

  /**
   * [protected description]
   * @var array
   */
  protected $data;

  /**
   * @param Schema $schema
   * @param mixed  $data
   */
  public function __construct(Schema $schema, $data)
  {
    $this->schema = $schema;
    $this->data = $data;
  }

  /**
   * [toDataColumns description]
   * @return DataColumn[]
   */
  public function toDataColumns(): array {
    $columns = [];
    foreach($this->schema->fields as $field) {
      $this->process($field, $columns, [$field->name]);
    }
    return $columns;
  }

  /**
   * [process description]
   * @param Field         $field    [description]
   * @param DataColumn[]  &$columns  [description]
   * @param array         $path
   * @param array         $options
   */
  protected function process(Field $field, array &$columns, array $path, array $options = []): void {
    switch($field->schemaType) {
      case SchemaType::Data:
        $this->processData($field, $columns, $path, $options);
        break;
      case SchemaType::Map:
        $this->processMap($field, $columns, $path, $options);
        break;
      case SchemaType::List:
        $this->processList($field, $columns, $path, $options);
        break;
      case SchemaType::Struct:
        $this->processStruct($field, $columns, $path, $options);
        break;

      default:
        throw new Exception('Unsupported SchemaType '.$field->schemaType);
    }
  }

  /**
   * [processData description]
   * @param DataField     $df         [description]
   * @param DataColumn[]  &$columns   [description]
   * @param array         $path
   * @param array         $options
   */
  protected function processData(DataField $df, array &$columns, array $path, array $options = []): void {

    // info, if DF values are nullable
    $options['nullable_levels'][] = $df->hasNulls;
    $options['repeated_levels'][] = $df->isArray; // Unsure about the requirement of this one...

    // detect whether we have repeated levels somewhere in the tree
    $hasRepetitions = array_search(true, $options['repeated_levels'] ?? [], true) !== false;

    $data = [];
    $definitionLevels = [];
    $repetitionLevels = $hasRepetitions ? [] : null;

    $options['max_depth'] = max(array_keys($path));

    //
    // prepare maximum DL/RL per level
    //
    $options['max_dl_per_level'] = [];
    $options['max_rl_per_level'] = [];
    $tMaxDl = 0;
    $tMaxRl = 0;
    for ($lv=0; $lv <= $options['max_depth']; $lv++) {
      $tMaxDl += (($options['nullable_levels'][$lv] ?? null) ? 1 : 0);
      $tMaxRl += (($options['repeated_levels'][$lv] ?? null) ? 1 : 0);
      $options['max_dl_per_level'][$lv] = $tMaxDl;
      $options['max_rl_per_level'][$lv] = $tMaxRl;
    }

    foreach($this->data as $rowIndex => $dataset) {
      static::processDataRecursively($dataset, $path, $options, $definitionLevels, $repetitionLevels, 0, 0, $data);
    }

    $dataColumn = new DataColumn($df, $data);

    // Only write definition and repetition levels, if applicable
    $dataColumn->definitionLevels = $df->maxDefinitionLevel > 0 ? $definitionLevels : null;
    $dataColumn->repetitionLevels = $df->maxRepetitionLevel > 0 ? $repetitionLevels : null;

    // add to final columns
    $columns[] = $dataColumn;
  }

  /**
   * [processDataRecursively description]
   * @param  array      $array                          [description]
   * @param  array      $path                           [description]
   * @param  array      $options                        [description]
   * @param  array      &$definitionLevels               [description]
   * @param  array|null &$repetitionLevels               [description]
   * @param  int        $level                          [description]
   * @param  int        $rl                             [description]
   * @param  array      &$data                           [description]
   * @return void
   */
  protected static function processDataRecursively(array $array, array $path, array $options, array &$definitionLevels, ?array &$repetitionLevels, int $level, int $rl, array &$data): void {
    $key = $path[$level] ?? null;
    $isMaxDepth = ($level === $options['max_depth']);
    $nullable = $options['nullable_levels'][$level] ?? null;
    $repeated = $options['repeated_levels'][$level] ?? null;

    $currentMaxDl = $options['max_dl_per_level'][$level];
    $currentMaxRl = $options['max_rl_per_level'][$level];

    // empty array handling right here
    if(count($array) === 0) {
      $data[] = null;
      $definitionLevels[] = $currentMaxDl - ($repeated ? 1 : 0);

      // Write RLs, if applicable
      if($repetitionLevels !== null) {
        $repetitionLevels[] = $rl;
      }
      return;
    }

    if($isMaxDepth) {
      //
      // Maximum recursion level
      //
      if($key) {
        //
        // Explicit key
        //
        $v = $array[$key];
        if($v !== null) {
          // Explicit value, maximum definition level
          $data[] = $v;
          $definitionLevels[] = $currentMaxDl;
        } else {
          if($nullable) {
            // Nullable field, null value leads to maxDl-1
            $data[] = null;
            $definitionLevels[] = $currentMaxDl - 1;
          } else {
            throw new \Exception('Malformed data: null value in not-nullable field');
          }
        }

        // Write-out RLs, if applicable
        if($repetitionLevels !== null) {
          $repetitionLevels[] = $rl;
        }
      } else {
        //
        // Keyless (e.g. array item/repeated fields)
        // Also includes raw data of map fields
        //

        // Map field's key & value fields level handling
        $isKeyLevel = ($options['key_level'] ?? null) && ($options['key_level'] === $level);
        $isValueLevel = ($options['value_level'] ?? null) && ($options['value_level'] === $level);

        // NOTE: we re-assign $array here
        if($isKeyLevel) {
          // If we're dealing with a key-field's data
          // extract keys only
          $array = array_keys($array);
        } else if($isValueLevel) {
          // if we're dealing with a value-field's data
          // extract values only (to a regular array)
          $array = array_values($array);
        }

        // empty array handling
        if(count($array) === 0) {
          $data[] = null;
          $definitionLevels[] = $currentMaxDl-1; // TODO only if nullable?

          if($repetitionLevels !== null) {
            $repetitionLevels[] = $rl;
          }
          return; // data written, return here.
        }

        // Track iteration has started
        $started = false;

        // Iterate over value array
        foreach($array as $v) {
          if($v !== null) {
            // Explicit value => maxDl
            $data[] = $v;
            $definitionLevels[] = $currentMaxDl;

          } else {
            if($nullable) {
              // null value and nullable field => maxDl-1
              $data[] = null;
              $definitionLevels[] = $currentMaxDl-1;
            } else {
              throw new \Exception('Malformed data: null value/item in not-nullable field/array');
            }
          }

          // Write-out RLs, if applicable
          if($repetitionLevels !== null) {
            //
            // If we didn't start writing yet,
            // we continue given the recent RL
            // e.g. 0 if its a new row
            // or more, if we're continuing in a nested array/repeated field
            // which may also be the maxRl itself (the 'most-inner' array/repeated field)
            //
            $repetitionLevels[] = $started ? $currentMaxRl : $rl;
          }

          // set iteration start tracker
          $started = true;
        }
      }
    } else {

      // Override array value
      if($key !== null) {
        $array = $array[$key];
      }

      if($repeated) {
        if($array === null) {
          if(!$nullable) {
            throw new \Exception('Malformed data: null value in not-nullable intermediary field/array');
          }
          $data[] = null;
          $definitionLevels[] = $currentMaxDl - 1;

          // Write RLs, if applicable
          if($repetitionLevels !== null) {
            $repetitionLevels[] = $rl > $currentMaxRl ? $currentMaxRl : $rl;
          }
        } else if(count($array) === 0) {
          // empty array
          if(!$nullable) {
            throw new \Exception('Malformed data: empty array/list in non-empty intermediary field/array');
          }

          $data[] = null;
          $definitionLevels[] = $currentMaxDl - 1; // WARNING?? TODO: we might have to react to NULLABLE flag $dl + 1;

          // Write RLs, if applicable
          if($repetitionLevels !== null) {
            $repetitionLevels[] = $rl > $currentMaxRl ? $currentMaxRl : $rl;
          }
        } else {

          if(($level+1) === $options['max_depth']) {
            //
            // Next level is maximum nesting level
            //
            static::processDataRecursively($array, $path, $options, $definitionLevels, $repetitionLevels, $level+1, $rl, $data);
          } else {
            // Iteration start tracking
            // for continuing a previous RL or not
            $started = false;
            foreach($array as $v) {
              if($v !== null) {
                static::processDataRecursively($v, $path, $options, $definitionLevels, $repetitionLevels, $level+1, $started ? $currentMaxRl : $rl, $data);
              } else {
                // null value...
                $data[] = null;
                $definitionLevels[] = $currentMaxDl; // WARNING?? $dl;

                // Write RLs, if applicable
                if($repetitionLevels !== null) {
                  $repetitionLevels[] = $started ? $currentMaxRl : $rl;
                }
              }

              // track iterations start
              $started = true;
            }
          }
        }
      } else {
        // no repetition
        // array is either a sub-array (assoc)
        //
        if($array === null) {
          if(!$nullable) {
            throw new \Exception('Malformed data: null value in not-nullable intermediary field');
          }

          $data[] = null;
          $definitionLevels[] = $currentMaxDl - 1;
          if($repetitionLevels !== null) {
            $repetitionLevels[] = $rl;
          }
        } else {
          // Recurse one level deeper given same array
          // This is a pseudo key in $path
          static::processDataRecursively($array, $path, $options, $definitionLevels, $repetitionLevels, $level+1, $rl, $data);
        }
      }
    }
  }

  /**
   * [processMap description]
   * @param MapField      $mf         [description]
   * @param DataColumn[]  &$columns   [description]
   * @param array         $path
   * @param array         $options
   */
  protected function processMap(MapField $mf, array &$columns, array $path, array $options = []): void {
    // add current field to nullable levels (as it is nullable)
    $options['nullable_levels'][] = $mf->hasNulls;
    $options['nullable_levels'][] = $mf->keyValueHasNulls;

    // the KV-element is the repeated element
    $options['repeated_levels'][] = false; // map TESTING, can it be repeated on its own?
    $options['repeated_levels'][] = true; // key_value

    // keyvalue field itself has no path.
    $mfPath = $path;
    $mfPath[] = null;

    // unsure about this:
    // if($mf->isArray) {
    //   $mfPath[] = null;
    // }

    // special handling for those fields
    $keyOptions = $options;
    $keyOptions['key_level'] = count($mfPath);
    $keyOptions['value_level'] = null;
    $keyPath = $mfPath;
    $keyPath[] = null;
    $this->process($mf->key, $columns, $keyPath, $keyOptions);

    $valueOptions = $options;
    $valueOptions['value_level'] = count($mfPath);
    $valueOptions['key_level'] = null;
    $valuePath = $mfPath;
    $valuePath[] = null;
    $this->process($mf->value, $columns, $valuePath, $valueOptions);
  }

  /**
   * [processList description]
   * @param ListField     $lf         [description]
   * @param DataColumn[]  &$columns   [description]
   * @param array         $path
   * @param array         $options
   */
  protected function processList(ListField $lf, array &$columns, array $path, array $options = []): void {
    $itemField = $lf->item;

    // add current field to nullable levels (as it is nullable)
    $options['nullable_levels'][] = $lf->hasNulls;
    $options['nullable_levels'][] = true; // list element nullability?!

    $options['repeated_levels'][] = false; // QUESTION: can lists be repeated on their own?
    $options['repeated_levels'][] = true; // list

    // item itself has no path.
    $fieldPath = $path;
    $fieldPath[] = null;
    $fieldPath[] = null;

    $this->process($itemField, $columns, $fieldPath, $options);
  }

  /**
   * [processStruct description]
   * @param StructField   $sf         [description]
   * @param DataColumn[]  &$columns   [description]
   * @param array         $path
   * @param array         $options
   */
  protected function processStruct(StructField $sf, array &$columns, array $path, array $options = []): void {
    $fields = $sf->getFields();

    $options['nullable_levels'][] = $sf->hasNulls;
    $options['repeated_levels'][] = $sf->isArray;

    $fieldPath = $path;
    if($sf->isArray) {
      $options['nullable_levels'][] = false; // fix for bug related to repeated structs?
      $fieldPath[] = null;
    }

    foreach($fields as $field) {
      // $fieldOptions = $options;
      $tPath = $fieldPath;
      $tPath[] = $field->name;
      $this->process($field, $columns, $tPath, $options);
    }
  }

}
