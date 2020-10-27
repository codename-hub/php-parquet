<?php
namespace jocoon\parquet\data\concrete;

use Exception;

use jocoon\parquet\data\Field;
use jocoon\parquet\data\Schema;
use jocoon\parquet\data\MapField;
use jocoon\parquet\data\SchemaType;
use jocoon\parquet\data\NonDataDataTypeHandler;

use jocoon\parquet\format\ConvertedType;

class MapDataTypeHandler extends NonDataDataTypeHandler
{
  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
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
    $map->path = $tseRoot->name . Schema::PathSeparator . $tseContainer->name;

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
    \jocoon\parquet\data\Field $field,
    \jocoon\parquet\format\SchemaElement $parent,
    array &$container
  ): void {
    throw new \LogicException('Not implemented'); // TODO
  }
}
