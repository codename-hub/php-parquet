<?php
namespace jocoon\parquet\data;

use jocoon\parquet\ParquetOptions;

use jocoon\parquet\adapter\BinaryReader;
use jocoon\parquet\adapter\BinaryWriter;

use jocoon\parquet\format\SchemaElement;

interface DataTypeHandlerInterface
{
  /**
   * [isMatch description]
   * @param  SchemaElement        $tse           [description]
   * @param  ParquetOptions|null  $formatOptions [description]
   * @return bool                          [description]
   */
  function isMatch(SchemaElement $tse, ?ParquetOptions $formatOptions) : bool;

  /**
   * [createSchemaElement description]
   * @param  SchemaElement[]  $schema          [description]
   * @param  int    &$index           [description]
   * @param  int    &$ownedChildCount [description]
   * @return Field
   */
  function createSchemaElement(array $schema, int &$index, int &$ownedChildCount) : Field;

  function getDataType() : int;

  function getSchemaType() : int;

  /**
   * [mergeDictionary description]
   * @param  array $dictionary [description]
   * @param  int[] $indexes    [description]
   * @param  array &$data       [description]
   * @param  int   $offset     [description]
   * @param  int   $length     [description]
   * @return array             [description]
   */
  function mergeDictionary(array $dictionary, array $indexes, array &$data, int $offset, int $length): array;

  // function getArray(int $minCount, bool $rent, bool $isNullable) : array;
  //
  function read(BinaryReader $reader, SchemaElement $tse, array &$dest, int $offset) : int;
  //
  function readObject(BinaryReader $reader, SchemaElement $tse, int $length);

  /**
   * [unpackDefinitions description]
   * @param  array $src                [description]
   * @param  int[] $definitionLevels   [description]
   * @param  int   $maxDefinitionLevel [description]
   * @param  array &$hasValueFlags      [description]
   * @return array                     [description]
   */
  function unpackDefinitions(array $src, array $definitionLevels, int $maxDefinitionLevel, array &$hasValueFlags) : array;

  /**
   * [packDefinitions description]
   * @param  array $data               [description]
   * @param  int   $maxDefinitionLevel [description]
   * @param  int[] &$definitions        [description]
   * @param  int   &$definitionsLength  [description]
   * @param  int   &$nullCount          [description]
   * @return array                     [description]
   */
  function packDefinitions(array $data, int $maxDefinitionLevel, array &$definitions, int &$definitionsLength, int &$nullCount) : array;

  /**
   * [write description]
   * @param SchemaElement        $tse        [description]
   * @param BinaryWriter         $writer     [description]
   * @param array                $values     [description]
   * @param DataColumnStatistics $statistics [description]
   */
  function write(SchemaElement $tse, BinaryWriter $writer, array $values, DataColumnStatistics $statistics = null): void;

  /**
   * [createThrift description]
   * @param Field             $field     [description]
   * @param SchemaElement     $parent    [description]
   * @param SchemaElement[]   &$container [description]
   */
  function createThrift(Field $field, SchemaElement $parent, array &$container): void;

  function plainEncode(SchemaElement $tse, $x);

  function plainDecode(SchemaElement $tse, $encoded);

}
