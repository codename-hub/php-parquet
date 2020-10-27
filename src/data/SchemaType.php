<?php
namespace jocoon\parquet\data;

/**
 * [abstract description]
 */
abstract class SchemaType
{
  /**
   * [Data description]
   * @var int
   */
  const Data = 1;

  /**
   * [Map description]
   * @var int
   */
  const Map = 2;

  /**
   * [Struct description]
   * @var int
   */
  const Struct = 3;

  /**
   * [List description]
   * @var int
   */
  const List = 4;
}
