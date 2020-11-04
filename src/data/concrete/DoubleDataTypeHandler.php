<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

class DoubleDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'double';
    parent::__construct(DataType::Double, Type::DOUBLE);
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) : float {
    return $reader->readDouble();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\jocoon\parquet\adapter\BinaryWriter $writer, $value): void
  {
    $writer->writeDouble($value);
  }
}
