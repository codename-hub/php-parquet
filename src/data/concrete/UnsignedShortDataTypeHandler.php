<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

/**
 * "UnsignedShort"/"ushort" is synonymous for an UInt16
 */
class UnsignedShortDataTypeHandler extends UnsignedInt16DataTypeHandler
{
  /**
   */
  public function __construct()
  {
    parent::__construct();
    $this->dataType = DataType::UnsignedShort;
  }
}
