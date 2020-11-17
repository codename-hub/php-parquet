<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use DateTimeImmutable;

use jocoon\parquet\ParquetOptions;

use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataColumn;

final class StatisticsTest extends TestBase
{
  /**
   * [$NameToTest description]
   * @var TestDesc[]
   */
  static $NameToTest = null;

  /**
   * [testDistinctStatForBasicDataTypes description]
   */
  public function testDistinctStatForBasicDataTypes(): void {
    static::$NameToTest = static::$NameToTest ?? [
      "int" => new TestDesc([
        'Type'          => 'integer', // NOTE: not nullable, see below
        'HasNulls'      => false,
        'Data'          => [ 4, 2, 1, 3, 5, 1, 4 ],
        'NullCount'     => 0,
        'DistinctCount' => 5,
        'Min'           => 1,
        'Max'           => 5
      ]),
      "int?" => new TestDesc([
        'Type'          => 'integer', // NOTE: nullable, see below
        'HasNulls'      => true,
        'Data'          => [ 4, 2, 1, 3, 1, null, 4 ],
        'DistinctCount' => 4,
        'NullCount'     => 1,
        'Min'           => 1,
        'Max'           => 4
      ]),
      "string" => new TestDesc([
        'Type'          => 'string', // typeof(string),
        'Data'          => [ "one", "two", "one" ],
        'NullCount'     => 0,
        'DistinctCount' => 2,
        'Min'           => "one",
        'Max'           => "two"
      ]),
      "string2" => new TestDesc([
        'Type'          => 'string', // typeof(string),
        'Data'          => [ "one", "two", "one", "three" ],
        'NullCount'     => 0,
        'DistinctCount' => 3,
        'Min'           => "one",
        'Max'           => "two" // yes, it is!
      ]),
      "string2" => new TestDesc([
        'Type'          => 'string', // typeof(string),
        'HasNulls'      => true,
        'Data'          => [ "one", "two", "one", "three", null, "zzz" ],
        'NullCount'     => 1,
        'DistinctCount' => 4,
        'Min'           => "one",
        'Max'           => "zzz"
      ]),
      "float" => new TestDesc([
        'Type'          => 'float', // typeof(float),
        'Delta'         => 0.000001,
        'Data'          => [ 1.23, 2.1, 0.5, 0.5 ],
        'DistinctCount' => 3,
        'NullCount'     => 0,
        'Min'           => 0.5,
        'Max'           => 2.1 // This causes a problem on certain systems - floating point fluctuation!
      ]),
      "double" => new TestDesc([
        'Type'          => 'double', // typeof(double),
        'Data'          => [ 1.23, 2.1, 0.5, 0.5 ],
        'DistinctCount' => 3,
        'NullCount'     => 0,
        'Min'           => 0.5,
        'Max'           => 2.1
      ]),
      "dateTime" => new TestDesc([
        'Type' => \DateTimeImmutable::class, // 'DateTimeImmutable',
        'Data' => [
          new DateTimeImmutable('2019-12-16 00:00:00'),
          new DateTimeImmutable('2019-12-16 00:00:00'),
          new DateTimeImmutable('2019-12-15 00:00:00'),
          new DateTimeImmutable('2019-12-17 00:00:00'),
        ],
        'DistinctCount' => 3,
        'NullCount'     => 0,
        'Min'           => new DateTimeImmutable('2019-12-15 00:00:00'),
        'Max'           => new DateTimeImmutable('2019-12-17 00:00:00')
      ]),
      "dateTimeWithOffset" => new TestDesc([
        'Type' => \DateTimeImmutable::class,
        'Data' => [
          new DateTimeImmutable('2019-12-16 05:00:00'),
          new DateTimeImmutable('2019-12-16 05:00:00'),
          new DateTimeImmutable('2019-12-15 05:00:00'),
          new DateTimeImmutable('2019-12-17 05:00:00'),
        ],
        'DistinctCount' => 3,
        'NullCount' => 0,
        'Min' => new DateTimeImmutable('2019-12-15 05:00:00'),
        'Max' => new DateTimeImmutable('2019-12-17 05:00:00')
      ]),
      "dateTimeNullable" => new TestDesc([
        'Type'      => \DateTimeImmutable::class, // 'DateTimeImmutable',
        'HasNulls'  => true,
        'Data' => [
          new DateTimeImmutable('2019-12-16 00:00:00'),
          new DateTimeImmutable('2019-12-16 00:00:00'),
          null,
          new DateTimeImmutable('2019-12-15 00:00:00'),
          new DateTimeImmutable('2019-12-17 00:00:00'),
        ],
        'DistinctCount' => 3,
        'NullCount'     => 1,
        'Min'           => new DateTimeImmutable('2019-12-15 00:00:00'),
        'Max'           => new DateTimeImmutable('2019-12-17 00:00:00')
      ]),

    ];

    foreach(array_keys(static::$NameToTest) as $key) {
      $this->Distinct_stat_for_basic_data_types($key);
    }
  }

  /**
   * [Distinct_stat_for_basic_data_types description]
   * @param string $name [description]
   */
  protected function Distinct_stat_for_basic_data_types(string $name): void
  {
    $test = static::$NameToTest[$name];

    if($test->Field) {
      $id = $test->Field;
    } else {
      $id = DataField::createFromType("id", $test->Type);
    }

    if($test->HasNulls !== null) {
      $id->hasNulls = $test->HasNulls;
    }

    //
    // Enable statistics calculation
    //
    $options = new ParquetOptions();
    $options->CalculateStatistics = true;

    $rc = $this->WriteReadSingleColumn($id, new DataColumn($id, $test->Data), $options);

    $this->assertEquals(count($test->Data), $rc->CalculateRowCount(), "{$name} calculated row count");
    $this->assertEquals($test->DistinctCount, $rc->statistics->distinctCount, "{$name} distinctCount"); // .Statistics.DistinctCount
    $this->assertEquals($test->NullCount, $rc->statistics->nullCount, "{$name} nullCount");

    if($test->Delta !== null) {
      $this->assertEqualsWithDelta($test->Min, $rc->statistics->minValue, $test->Delta, "{$name} minValue");
      $this->assertEqualsWithDelta($test->Max, $rc->statistics->maxValue,$test->Delta, "{$name} maxValue");
    } else {
      $this->assertEquals($test->Min, $rc->statistics->minValue, "{$name} minValue");
      $this->assertEquals($test->Max, $rc->statistics->maxValue, "{$name} maxValue");
    }

  }
}

/**
 * [TestDesc description]
 */
class TestDesc
{
  /**
   * [__construct description]
   * @param [type] $params [description]
   */
  public function __construct($params = null)
  {
    if($params) {
      $this->Field = $params['Field'] ?? null;
      $this->Type = $params['Type'] ?? null;
      $this->Delta = $params['Delta'] ?? null;
      $this->Data = $params['Data'] ?? null;
      $this->HasNulls = $params['HasNulls'] ?? null;
      $this->DistinctCount = $params['DistinctCount'] ?? null;
      $this->NullCount = $params['NullCount'] ?? null;
      $this->Min = $params['Min'] ?? null;
      $this->Max = $params['Max'] ?? null;
    }
  }

  /**
   * [public description]
   * @var DataField
   */
  public $Field;

  /**
   * PHP type or class used for values
   * @var string
   */
  public $Type;

  /**
   * Precision/Delta for value comparison (floats & doubles)
   * @var float
   */
  public $Delta;

  /**
   * [public description]
   * @var bool
   */
  public $HasNulls;

  /**
   * Data/Values
   * @var array
   */
  public $Data;

  /**
   * Distinct/unique values count
   * @var int
   */
  public $DistinctCount;

  /**
   * Count of nulls
   * @var int
   */
  public $NullCount;

  /**
   * Minimum/lowest value
   * @var mixed
   */
  public $Min;

  /**
   * Maximum/highest value
   * @var mixed
   */
  public $Max;
}
