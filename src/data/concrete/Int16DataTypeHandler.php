<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

class Int16DataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    // NOTE: we do not set $this->phpType here, as it's not a native PHP type.
    parent::__construct(DataType::Int16, Type::INT32, ConvertedType::INT_16);
  }

  /**
   * Maximum value for datatype
   * @var int
   */
  const MaxValue = 32767;

  /**
   * Minimum value for datatype
   * @var int
   */
  const MinValue = -32768;

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \codename\parquet\adapter\BinaryReader $reader,
    \codename\parquet\format\SchemaElement $tse,
    int $length
  ) {
    // TODO: checks for INT16
    return $reader->readInt32();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\codename\parquet\adapter\BinaryWriter $writer, $value): void
  {
    if((static::MinValue <= $value) && ($value <= static::MaxValue)) {
      $writer->writeInt32($value);
    } else {
      throw new \Exception('Value out of Int16 bounds');
    }
  }
}
