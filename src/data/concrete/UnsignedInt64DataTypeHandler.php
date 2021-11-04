<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

/**
 * This is a preliminary DataTypeHandler
 * as PHP doesn't support 'real' UInt64 natively, no pun intended.
 * Suprisingly, you might stumble upon huge integers being converted to a float,
 * so this is still some kind of pun.
 * Recommendation: do not use this at the moment.
 */
class UnsignedInt64DataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    // NOTE: we do not set $this->phpType here, as it's not a native PHP type.
    parent::__construct(DataType::UnsignedInt64, Type::INT64, ConvertedType::UINT_64);
  }

  /**
   * Maximum value for datatype
   * @var int
   */
  const MaxValue = 18446744073709551615;

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
    return $reader->readUInt64();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\codename\parquet\adapter\BinaryWriter $writer, $value): void
  {
    if((static::MinValue <= $value) && ($value <= static::MaxValue)) {
      $writer->writeUInt64($value);
    } else {
      throw new \Exception('Value out of UInt64 bounds');
    }
  }
}
