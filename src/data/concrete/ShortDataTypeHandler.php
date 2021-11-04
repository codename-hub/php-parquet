<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

/**
 * "Short" is synonymous for an Int16
 */
class ShortDataTypeHandler extends Int16DataTypeHandler
{
  /**
   */
  public function __construct()
  {
    parent::__construct();

    // Override datatype, synonymous
    $this->dataType = DataType::Short;
  }
}
