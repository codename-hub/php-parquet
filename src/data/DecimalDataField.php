<?php
namespace jocoon\parquet\data;

use Exception;

class DecimalDataField extends DataField
{
  /**
   * [public description]
   * @var int
   */
  public $precision;

  /**
   * [public description]
   * @var int
   */
  public $scale;

  /**
   * [public description]
   * @var bool
   */
  public $forceByteArrayEncoding;

  /**
   * @inheritDoc
   */
  public static function create(
    string $name,
    int $precision,
    int $scale,
    bool $forceByteArrayEncoding = false,
    bool $hasNulls = true,
    bool $isArray = false
  ): self {
    $dataField = new self($name, DataType::Decimal, $hasNulls, $isArray);

    if($precision < 1) throw new Exception("precision cannot be less than 1");
    if($scale < 1) throw new Exception("scale cannot be less than 1");

    $dataField->precision = $precision;
    $dataField->scale = $scale;
    $dataField->forceByteArrayEncoding = $forceByteArrayEncoding;
    return $dataField;
  }
}
