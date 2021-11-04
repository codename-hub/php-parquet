<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

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
    \codename\parquet\adapter\BinaryReader $reader,
    \codename\parquet\format\SchemaElement $tse,
    int $length
  ) {
    // We're always reading INT32, see Thrift Type
    // TODO: Check whether we'd need some unpacking magic
    return $reader->readInt32();
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\codename\parquet\adapter\BinaryWriter $writer, $value): void
  {
    // We're always writing INT32, see Thrift Type
    $writer->writeInt32($value);
  }

}
