<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

class ByteDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'string'; // ?
    parent::__construct(DataType::Byte, Type::INT32, ConvertedType::UINT_8);
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    // We're always reading INT32, see Thrift Type
    // TODO: Check whether we'd need some unpacking magic
    return $reader->readInt32();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\jocoon\parquet\adapter\BinaryWriter $writer, $value): void
  {
    // We're always writing INT32, see Thrift Type
    $writer->writeInt32($value);
  }

}
