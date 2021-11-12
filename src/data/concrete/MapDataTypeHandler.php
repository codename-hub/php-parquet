<?php
namespace codename\parquet\data\concrete;

use Exception;

use codename\parquet\data\Field;
use codename\parquet\data\Schema;
use codename\parquet\data\MapField;
use codename\parquet\data\SchemaType;
use codename\parquet\data\NonDataDataTypeHandler;

use codename\parquet\format\ConvertedType;

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
    throw new \LogicException('Not implemented'); // TODO
  }
}
