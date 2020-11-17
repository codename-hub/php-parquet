<?php
namespace jocoon\parquet\data;

abstract class NonDataDataTypeHandler implements DataTypeHandlerInterface
{
  /**
   * @inheritDoc
   */
  public function getDataType(): int
  {
    return DataType::Unspecified;
  }

  /**
   * @inheritDoc
   */
  public function read(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    array &$dest,
    int $offset
  ): int {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function readObject(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function mergeDictionary(
    array $dictionary,
    array $indexes,
    array &$data,
    int $offset,
    int $length
  ): array {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function unpackDefinitions(
    array $src,
    array $definitionLevels,
    int $maxDefinitionLevel,
    array &$hasValueFlags
  ): array {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function packDefinitions(
    array $data,
    int $maxDefinitionLevel,
    array &$definitions,
    int &$definitionsLength,
    int &$nullCount
  ): array {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function write(
    \jocoon\parquet\format\SchemaElement $tse,
    \jocoon\parquet\adapter\BinaryWriter $writer,
    array $values,
    \jocoon\parquet\data\DataColumnStatistics $statistics = null
  ): void {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function plainEncode(\jocoon\parquet\format\SchemaElement $tse, $x)
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function plainDecode(
    \jocoon\parquet\format\SchemaElement $tse,
    $encoded
  ) {
    throw new \LogicException('Not implemented'); // TODO
  }
}
