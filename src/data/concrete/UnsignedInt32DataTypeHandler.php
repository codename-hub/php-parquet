<?php
namespace codename\parquet\data\concrete;

use Nelexa\Buffer\Cast;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

class UnsignedInt32DataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    // NOTE: we do not set $this->phpType here, as it's not a native PHP type.
    parent::__construct(DataType::UnsignedInt32, Type::INT32, ConvertedType::UINT_32);
  }

  /**
   * Maximum value for datatype
   * @var int
   */
  const MaxValue = 4294967295;

  /**
   * Minimum value for datatype
   * @var int
   */
  const MinValue = 0;

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \codename\parquet\adapter\BinaryReader $reader,
    \codename\parquet\format\SchemaElement $tse,
    int $length
  ) {
    return $reader->readUInt32();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\codename\parquet\adapter\BinaryWriter $writer, $value): void
  {
    if((static::MinValue <= $value) && ($value <= static::MaxValue)) {
      $writer->writeUInt32($value);
    } else {
      throw new \Exception('Value out of UInt32 bounds');
    }
  }
}
