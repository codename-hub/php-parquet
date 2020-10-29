<?php
namespace jocoon\parquet\data;

class DateTimeDataField extends DataField
{
  /**
   * [public description]
   * @var int
   */
  public $dateTimeFormat;

  /**
   * [create description]
   * @param  string  $name     [description]
   * @param  int     $format   [description]
   * @param  bool    $hasNulls [description]
   * @param  bool    $isArray  [description]
   * @return self              [description]
   */
  public static function create(
    string $name,
    $format,
    bool $hasNulls = true,
    bool $isArray = false
  ): self {
    $field = new self($name, DataType::DateTimeOffset, $hasNulls, $isArray);
    $field->dateTimeFormat = $format;
    return $field;

  }
}
