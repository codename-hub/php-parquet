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
 * Convert a parquet file's Schema+DataColumns
 * to the equivalent PHP data, row-style
 */
class DataColumnsToArrayConverter
{
  /**
   * [protected description]
   * @var Schema
   */
  protected $schema;

  /**
   * [protected description]
   * @var DataColumn[]
   */
  protected $columns;

  /**
   * [protected description]
   * @var int|null
   */
  protected $totalRowCount;

  /**
   * [__construct description]
   * @param Schema        $schema         [description]
   * @param DataColumn[]  $columns        [description]
   * @param int|null      $totalRowCount  [description]
   */
  public function __construct(Schema $schema, array $columns, ?int $totalRowCount = null)
  {
    $this->validateColumnsAreInSchema($schema, $columns);

    $this->schema = $schema;
    $this->columns = $columns;
    $this->totalRowCount = $totalRowCount;
  }

  /**
   * Returns a PHP array with all the datasets
   * @return array
   */
  public function toArray(): array {
    $result = [];
    foreach($this->schema->fields as $field) {
      $this->build($field, $result, [ $field->name ]);
    }
    return $result;
  }

  /**
   * Builds/integrates a given field's data into a result array
   * @param  Field  $field                 [description]
   * @param  array  &$result               [description]
   * @param  array  $path                  [current path]
   * @param  array  $options  [description]
   * @return void
   */
  protected function build(Field $field, array &$result, array $path = [], array $options = []): void {
    switch($field->schemaType) {
      case SchemaType::Data:
        $this->buildData($field, $result, $path, $options);
        break;
      case SchemaType::Map:
        $this->buildMap($field, $result, $path, $options);
        break;
      case SchemaType::List:
        $this->buildList($field, $result, $path, $options);
        break;
      case SchemaType::Struct:
        $this->buildStruct($field, $result, $path, $options);
        break;

      default:
        throw new Exception('Unsupported SchemaType '.$field->schemaType);
    }
  }

  /**
   * Returns the DataColumn of a given DataField
   * @param  DataField  $df [description]
   * @return DataColumn     [description]
   */
  protected function getDataColumn(DataField $df): DataColumn {
    foreach($this->columns as $column) {
      // NOTE: we have to compare paths to be sure.
      if($df->Equals($column->getField()) && ($df->path === $column->getField()->path)) {
        return $column;
      }
    }
    throw new \Exception('DataColumn not found for field: '.$df->name);
  }

  /**
   * Builds the data of a given DataField
   * @param DataField $df      [description]
   * @param array     &$result  [description]
   * @param array     $path
   * @param array     $options  [description]
   */
  protected function buildData(DataField $df, array &$result, array $path, array $options = []): void {
    $column = $this->getDataColumn($df);

    // Add the current Datafield to tracked nullable level data
    $options['nullable_levels'][] = $df->hasNulls;

    if($column->hasRepetitions()) {

      // The top-level DF repetition is already handled inside the DataTypeHandler
      // $options['repeated_levels'][] = $df->isArray;

      // see https://github.com/apache/parquet-cpp/blob/642da055adf009652689b20e68a198cffb857651/src/parquet/arrow/reader.cc#L798
      $valuesDefinedLevel = $df->maxDefinitionLevel;
      if($options['nullable_levels'][count($options['nullable_levels'])-1]) {
        $valuesDefinedLevel--;
      }

      // the raw data
      $data = $column->getData();

      // the target result row index
      $currentRowIndex = -1;

      // previous repetition level
      $prl = -1;

      foreach($data as $index => $v) {
        $dl = $column->definitionLevels[$index];
        $rl = $column->repetitionLevels[$index];

        if(!$rl) {
          // start of a new record
          $currentRowIndex++;

          // reset ALL value indexes
          for ($i=1; $i <= $df->maxRepetitionLevel; $i++) {
            $valueIndexes[$i] = 0;
          }
        } else {
          // increase index at repetition level
          $valueIndexes[$rl]++;

          // reset all further indexes to 0, if any
          for ($i=$rl+1; $i <= $df->maxRepetitionLevel; $i++) {
            $valueIndexes[$i] = 0;
          }
        }

        $prl = $rl;

        // construct array path
        $vPath = [];
        $valueIndexesIndex = 1;

        $cDl = 0;

        // we could also use repeated_levels
        // just to termine maximum count of levels to handle
        $maxLv = count($options['nullable_levels']);

        for ($lv=0; $lv <= $maxLv; $lv++) {
          if(isset($path[$lv])) {
            $vPath[] = $path[$lv];
          }
          if($dl > $cDl && ($options['repeated_levels'][$lv] ?? false)) {
            $vPath[] = $valueIndexes[$valueIndexesIndex++];
          }
          $cDl += ($options['nullable_levels'][$lv] ?? null) ? 1 : 0;
          if($cDl > $dl) {
            break;
          }
        }

        if($options['repeated_levels'][$dl] ?? null) {
          // TODO!!!
          $result[$currentRowIndex] = DeepAccess::set($result[$currentRowIndex] ?? null, $vPath, []);
        } else {
          $result[$currentRowIndex] = DeepAccess::set($result[$currentRowIndex] ?? null, $vPath, $v);
        }

      }

    } else {
      // get the raw data of the column
      $data = $column->getData();

      if(count($path) > 1) {
        //
        // build a temporary pathmap
        // which maps DL => full target path
        // this is possible when there are no field repetitions
        //
        $dlPathmap = [];
        $pathIndex = 0;
        $currentDl = 0;
        $tPath = [];
        foreach($options['nullable_levels'] as $index => $nullable) {
          if($path[$pathIndex] !== null) {
            $tPath[] = $path[$pathIndex];
            // theres a named path
            if($nullable) {
              // nullable => one more DL!
              $dlPathmap[$currentDl] = $tPath;
              $dlPathmap[++$currentDl] = $tPath;
            } else {
              $dlPathmap[$currentDl] = $tPath;
            }
          }
          $pathIndex++;
        }

        if(count($dlPathmap) > 1) {
          // if there's more than one entry in the pathmap, we have to explicitly use it
          foreach($data as $index => $value) {
            $result[$index] = DeepAccess::set($result[$index] ?? null, $dlPathmap[$column->definitionLevels[$index]], $value);
          }
        } else {
          // pure run-through
          foreach($data as $index => $value) {
            $result[$index] = DeepAccess::set($result[$index] ?? null, $path, $value);
          }
        }
      } else {
        //
        // Field is at root level, no further nesting
        // for performance reasons, we directly set the field's value per row
        //
        $key = $path[0] ?? null;
        if($key) {
          foreach($data as $index => $value) {
            $result[$index][$key] = $value;
          }
        } else {
          // Special case, where we directly set the value.
          // This is legacy method for doing partial dataset reconstruction
          throw new \Exception('Legacy dataset reconstruction method, should not happen');
          // foreach($data as $index => $value) {
          //   $result[$index] = $value;
          // }
        }
      }
    }
  }

  /**
   * Builds a given MapField's data
   * @param  MapField $mf                   [description]
   * @param  array    &$result               [description]
   * @param  array    $path
   * @param  array    $options
   * @return void
   */
  protected function buildMap(MapField $mf, array &$result, array $path, array $options = []): void {
    if(!($mf->key instanceof DataField)) {
      // may be invalid anyways
      throw new \Exception('Map key field being a non-DataField is not supported');
    }

    $keyCollection = [];
    $valueCollection = [];

    // add current field to nullable levels (as it is nullable)
    $options['nullable_levels'][] = $mf->hasNulls;
    $options['nullable_levels'][] = $mf->keyValueHasNulls;

    // the KV-element is the repeated element
    $options['repeated_levels'][] = false; // map
    $options['repeated_levels'][] = true; // key_value

    // Track the recursion level
    // to improve the overall reconstruction process
    @$options['recursion_depth']++; // Suppress errors, key might be nonexistent

    $path[] = null;

    $this->build($mf->key, $keyCollection, $path, $options);
    $this->build($mf->value, $valueCollection, $path, $options);

    foreach($keyCollection as $index => $keys) {
      $values = $valueCollection[$index];
      $res = static::arrayCombineAtLevel($keys, $values, $options['recursion_depth'], 0);
      $result[$index] = (array_replace_recursive($result[$index] ?? [], $res));
    }
  }

  /**
   * Builds a given ListField's data
   * @param  ListField  $lf                   [description]
   * @param  array      &$result               [description]
   * @param  array      $path
   * @param  array      $options
   * @return void
   */
  protected function buildList(ListField $lf, array &$result, array $path, array $options = []): void {

    $itemField = $lf->item;

    // add current field to nullable levels (as it is nullable)
    $options['nullable_levels'][] = $lf->hasNulls;
    $options['nullable_levels'][] = true; // false; // list element nullability?! WARNING/TESTING

    $options['repeated_levels'][] = false;
    $options['repeated_levels'][] = true; // list

    // Track the recursion level
    // to improve the overall reconstruction process
    @$options['recursion_depth']++; // Suppress errors, key might be nonexistent

    $itemResult = [];
    $itemPath = $path;

    $itemPath[] = null;
    $itemPath[] = null; // array index

    $this->build($itemField, $itemResult, $itemPath, $options);

    // Propagate to result
    // in case of nested lists, $path might be [] and simply propagates like a pass-through
    foreach($itemResult as $index => $value) {
      // NOTE: we cannot set the path directly
      // as it must be handled in buildData() due to repetitions and nesting levels in-between

      // $result[$index] = array_replace_recursive($result[$index] ?? [], $value);
      // $result[$index] = DeepAccess::set($result[$index], $path, DeepAccess::get($value, $path));
      $result[$index] = array_replace_recursive($result[$index] ?? [], $value);
    }
  }

  /**
   * Builds a given StructField's data
   * @param  StructField  $sf                   [description]
   * @param  array        &$result               [description]
   * @param  array        $path
   * @param  array        $options
   * @return void
   */
  protected function buildStruct(StructField $sf, array &$result, array $path, array $options = []): void {
    $fields = $sf->getFields();

    $options['nullable_levels'][] = $sf->hasNulls;
    $options['repeated_levels'][] = $sf->isArray;
    @$options['recursion_depth']++; // Suppress errors, key might be nonexistent

    foreach($fields as $field) {
      $fieldPath = $path;
      $fieldPath[] = $field->name;
      $this->build($field, $result, $fieldPath, $options);
    }
  }

  /**
   * Recursive selective array_combine
   * that performs the combination at a specific level
   * if the level is _NOT_ reached during traversal
   * the respective data is given back
   * this also works for $values being NULL
   * @param  array|null   $keys                       [description]
   * @param  array|null   $values                     [description]
   * @param  int          $atLevel                    [description]
   * @param  int          $currentLevel               [description]
   * @return array|null
   */
  static function arrayCombineAtLevel(?array $keys, ?array $values, int $atLevel, int $currentLevel = 0): ?array {

    if($atLevel === $currentLevel) {
      if($keys === null) {
        return null;
      } else {
        // Legacy code, if needed:
        // if($values === null) {
        //   return array_fill_keys($keys, $values);
        // } else {
        //  ... <- do the regular combine here.
        // }

        // NOTE: We might combine [] and [] to []
        return array_combine($keys, $values);
      }
    } else {
      if($keys !== null) {
        $res = [];
        foreach($keys as $i => $k) {
          $v = $values[$i];
          $res[$i] = static::arrayCombineAtLevel($k, $v, $atLevel, $currentLevel+1);
        }
        return $res;
      } else {
        return null;
      }
    }
  }

  /**
   * Validates given schema against given DataColumns
   * @param Schema        $schema   [description]
   * @param DataColumn[]  $columns  [description]
   */
  protected function validateColumnsAreInSchema(Schema $schema, array $columns): void {
    $schemaFields = $schema->getDataFields();
    $passedFields = array_map(function(DataColumn $c) { return $c->getField(); }, $columns);

    if(count($schemaFields) !== count($passedFields)) {
      throw new \Exception('schema has '.count($schemaFields).' fields, but only '.count($passedFields).' are passed');
    }

    for ($i=0; $i < count($schemaFields); $i++) {
      $sf = $schemaFields[$i];
      $pf = $passedFields[$i];

      if(!$sf->Equals($pf)) {
        throw new \Exception('expected a different DataField at position '.$i);
      }
    }
  }
}
