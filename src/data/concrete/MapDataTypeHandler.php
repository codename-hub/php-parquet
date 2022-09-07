<?php
namespace codename\parquet\data\concrete;

use Exception;

use codename\parquet\data\Field;
use codename\parquet\data\Schema;
use codename\parquet\data\MapField;
use codename\parquet\data\SchemaType;
use codename\parquet\data\DataTypeFactory;
use codename\parquet\data\NonDataDataTypeHandler;

use codename\parquet\format\ConvertedType;
use codename\parquet\format\SchemaElement;
use codename\parquet\format\FieldRepetitionType;

class MapDataTypeHandler extends NonDataDataTypeHandler
{
  /**
   * @inheritDoc
   */
  public function isMatch(
    \codename\parquet\format\SchemaElement $tse,
    ?\codename\parquet\ParquetOptions $formatOptions
  ): bool {
    return isset($tse->converted_type) && ($tse->converted_type === ConvertedType::MAP || $tse->converted_type === ConvertedType::MAP_KEY_VALUE);
  }

  /**
   * @inheritDoc
   */
  public function createSchemaElement(
    array $schema,
    int &$index,
    int &$ownedChildCount
  ): Field {
    $tseRoot = $schema[$index];

    //next element is a container
    $tseContainer = $schema[++$index];

    if ($tseContainer->num_children != 2)
    {
      // throw new IndexOutOfRangeException("dictionary container must have exactly 2 children but {$tseContainer->num_children} found");
      throw new Exception("dictionary container must have exactly 2 children but {$tseContainer->num_children} found");
    }

    //followed by a key and a value, but we declared them as owned

    $map = new MapField($tseRoot->name);
    $map->setPath([ $tseRoot->name, $tseContainer->name ]);

    //
    // parquet-format spec:
    // [...] MAP that contains a single field named key_value.
    // The repetition of this level must be either optional or required and determines whether the list is nullable
    //
    $map->hasNulls = $tseRoot->repetition_type !== FieldRepetitionType::REQUIRED; // whether map itself is nullable
    $map->keyValueHasNulls = $tseContainer->repetition_type !== FieldRepetitionType::REQUIRED; // whether map values are nullable

    // QUESTION: can the map field itself can be repeated, in theory?
    // I don't think so, as the parquet format says you can't have
    // two repeated fields nested consecutively
    // $map->isArray = $tseRoot->repetition_type === FieldRepetitionType::REPEATED;

    $index += 1;
    $ownedChildCount = 2;
    return $map;
  }

  /**
   * @inheritDoc
   */
  public function getSchemaType(): int
  {
    return SchemaType::Map;
  }

  /**
   * @inheritDoc
   */
  public function createThrift(
    \codename\parquet\data\Field $field,
    \codename\parquet\format\SchemaElement $parent,
    array &$container
  ): void {
    $parent->num_children += 1;

    //add the root container where map begins
    $root = new SchemaElement([
      'name'            => $field->name,
      'converted_type'  => ConvertedType::MAP,
      'num_children'    => 1,
      'repetition_type' => $field->hasNulls ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED, // QUESTION: is it repeatable at this level?
    ]);

    $container[] = $root;

    //key-value is a container for column of keys and column of values
    $keyValue = new SchemaElement([
      'name'            => MapField::ContainerName,
      'num_children'    => 0, //is assigned by children
      'repetition_type' => FieldRepetitionType::REPEATED,
    ]);

    $container[] = $keyValue;

    //now add the key and value separately
    if($field instanceof MapField) {
      $keyHandler = DataTypeFactory::matchField($field->key);
      $valueHandler = DataTypeFactory::matchField($field->value);

      $keyHandler->createThrift($field->key, $keyValue, $container);
      $tseKey = $container[count($container) - 1];
      $valueHandler->createThrift($field->value, $keyValue, $container);
      $tseValue = $container[count($container) - 1];

      // fixups for weirdness in RLs
      // Ported from parquet-dotnet. Reasonable, as the key & value fields
      // must not be repeated.
      // NOTE: I think, as the key field must be required, we might have
      // to change this to FieldRepetitionType::REQUIRED
      // just for the key field itself.

      if($tseKey instanceof SchemaElement) {
        if($tseKey->repetition_type === FieldRepetitionType::REPEATED) {
          $tseKey->repetition_type = FieldRepetitionType::OPTIONAL;
        }
      }

      if($tseValue instanceof SchemaElement) {
        if($tseValue->repetition_type === FieldRepetitionType::REPEATED) {
          $tseValue->repetition_type = FieldRepetitionType::OPTIONAL;
        }
      }
    }
  }
}
