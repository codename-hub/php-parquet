<?php
namespace jocoon\parquet\data;

use jocoon\parquet\format\Statistics;
use jocoon\parquet\format\SchemaElement;

class DataColumnStatistics
{
  /**
   * [public description]
   * @var int
   */
  public $nullCount = 0;

  /**
   * [public description]
   * @var int
   */
  public $distinctCount = 0;

  /**
   * [public description]
   * @var mixed
   */
  public $minValue = null;

  /**
   * [public description]
   * @var mixed
   */
  public $maxValue = null;

  /**
   * @param int|null    $nullCount
   * @param int|null    $distinctCount
   * @param mixed|null  $minValue
   * @param mixed|null  $maxValue
   */
  public function __construct(
    int $nullCount = null,
    int $distinctCount = null,
    $minValue = null,
    $maxValue = null
  ) {
    $this->nullCount = $nullCount;
    $this->distinctCount = $distinctCount;
    $this->minValue = $minValue;
    $this->maxValue = $maxValue;
  }

  /**
   * [ToThriftStatistics description]
   * @param  DataTypeHandlerInterface $handler [description]
   * @param  SchemaElement            $tse     [description]
   * @return Statistics                        [description]
   */
  public function ToThriftStatistics(DataTypeHandlerInterface $handler, SchemaElement $tse): Statistics
  {
    $min = $handler->plainEncode($tse, $this->minValue);
    $max = $handler->plainEncode($tse, $this->maxValue);

    return new Statistics([
      'null_count' => $this->nullCount,
      'distinct_count' => $this->distinctCount,
      'min'       => $min,  // Deprecated, legacy field, set for compatibility
      'min_value' => $min,
      'max'       => $max,  // Deprecated, legacy field, set for compatibility.
      'max_value' => $max
    ]);
  }
}
