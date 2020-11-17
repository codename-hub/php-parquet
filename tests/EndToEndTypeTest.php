<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DateTimeFormat;
use jocoon\parquet\data\DecimalDataField;
use jocoon\parquet\data\DateTimeDataField;

final class EndToEndTypeTest extends TestBase
{
  /**
   * [protected description]
   * @var [type]
   */
  protected static $nameToData = null;

  /**
   * [createTestData description]
   */
  protected static function createTestData(): void {
    if(static::$nameToData !== null) {
      return;
    }

    static::$nameToData = [
      "plain string" => [
        'field'           => DataField::createFromType("plain string", 'string'),
        'expectedValue'   => "plain string"
      ],
      "unicode string" => [
        'field'           => DataField::createFromType("unicode string", 'string'),
        'expectedValue'   => "L'Oréal Paris"
      ],
      // "byte array" => [
      //   'field'         => DataField::createFromType("byte array", 'array', 'byte'),
      //   'expectedValue' => /*"raw byte string", // */ unpack('C*', "raw byte string") // Encoding.UTF8.GetBytes("raw byte string")
      // ],
      "float" => [
        'field'         => DataField::createFromType("float", 'float'),
        'expectedValue' => (float)1.23
      ],
      "float fluctuation" => [
        'field'         => DataField::createFromType("float", 'float'),
        'expectedValue' => (float)2.1,
        'delta'         => 0.000001 // Floating-point fluctuation delta
      ],
      "double" => [
        'field'         => DataField::createFromType("double", 'double'),
        'expectedValue' => (double)10.44
      ],
      "simple DateTime" => [
        'field'           => DataField::createFromType('datetime', \DateTimeImmutable::class),
        'expectedValue'   => new \DateTimeImmutable('now')
      ],
      "long" => [
        'field'         => DataField::createFromType("long", 'long'),
        'expectedValue' => (int)1234 // Long?
      ],

      // various cases of decimals
      "simple decimal" => [
        'field'         => DataField::createFromType("decDefault", 'decimal'),
        'expectedValue' => "123.4" // 123.4m
      ],
      "huge decimal" => [
        'field'         => DataField::createFromType("hugeDec", 'decimal'),
        'expectedValue' => "83086059037282.54" // 83086059037282.54m
      ],
      "int32 decimal" => [
        'field'         => DecimalDataField::create("decInt32", 4, 1),
        'expectedValue' => "12.4" // 12.4m
      ],
      "int64 decimal" => [
        'field'         => DecimalDataField::create("decInt64", 17, 12),
        'expectedValue' => "1234567.88" // 1234567.88m,
      ],
      "fixed byte array decimal" => [
        'field'         => DecimalDataField::create("decFixedByteArray", 48, 12),
        'expectedValue' => "34434.5" // 34434.5m
      ],
      "negative decimal" => [
        'field'         => DecimalDataField::create("decMinus", 10, 2, true),
        'expectedValue' => "-1" // -1m
      ],

      // might lose precision slightly, i.e.
      // Expected: 2017-07-13T10:58:44.3767154+00:00
      // Actual:   2017-07-12T10:58:44.3770000+00:00
      // NOTE: this seems not to be a problem in PHP ?
      "dateTimeOffset" => [
        'field'         => DataField::createFromType("dateTimeOffset", \DateTimeImmutable::class),
        'expectedValue' => (new \DateTimeImmutable('now')),
      ],
      "impala date" => [
        'field'         => DateTimeDataField::create("dateImpala", DateTimeFormat::Impala),
        'expectedValue' => (new \DateTimeImmutable('now')),
      ],
      "impala date explicit" => [
        // We're including this test, though it looks random.
        // This caused a major discrepancy, due to the non-strict nature of PHP, in conjunction with NanoTime
        'field'         => DataField::createFromType("dateImpalaExplicit", \DateTimeImmutable::class),
        'expectedValue' => new \DateTimeImmutable('2019-12-15 05:00:00'),
      ],
      "dateDateAndTime" => [
        'field'         => DateTimeDataField::create("dateDateAndTime", DateTimeFormat::DateAndTime),
        'expectedValue' => (new \DateTimeImmutable('now'))->setTimestamp(time()), // Modifying the DT via timestamp to get rid of the microseconds
      ],
      // don't want any excess info in the offset INT32 doesn't contain or care about this data
      "dateDate" => [
        'field'         => DateTimeDataField::create("dateDate", DateTimeFormat::Date),
        'expectedValue' => (new \DateTimeImmutable('now'))->setTime(0,0,0), // Set time to be all 0 (NOTE: check for microseconds behaviour?)
      ],
      // "interval" => [
      //   'field'         => DataField::createFromType("interval", 'Interval'),
      //   'expectedValue' => new Interval(3, 2, 1)
      // ],

      "byte min value" => [
        'field'         => DataField::createFromType("byte", 'byte'),
        'expectedValue' => 0 // 0x00 // byte.MinValue
      ],
      // TODO: byte reading/writing still causes errors
      // "byte max value" => [
      //   'field'         => DataField::createFromType("byte", 'byte'),
      //   'expectedValue' => 255 // 0xFF
      // ],

      // "signed byte min value" => [
      //   'field'         => DataField::createFromType("sbyte", 'sbyte'),
      //   'expectedValue' => sbyte.MinValue
      // ],
      // "signed byte max value" => [
      //   'field'         => DataField::createFromType("sbyte", 'sbyte'),
      //   'expectedValue' => sbyte.MaxValue
      // ],
      //
      // "short min value" => [
      //   'field'         => DataField::createFromType("short", 'short'),
      //   'expectedValue' => short.MinValue
      // ],
      // "short max value" => [
      //   'field'         => DataField::createFromType("short", 'short'),
      //   'expectedValue' => short.MaxValue
      // ],
      // "unsigned short min value" => [
      //   'field'         => DataField::createFromType("ushort", 'ushort'),
      //   'expectedValue' => ushort.MinValue
      // ],
      // "unsigned short max value" => [
      //   'field'         => DataField::createFromType("ushort", 'ushort'),
      //   'expectedValue' => ushort.MaxValue
      // ],

      "nullable decimal" => [
        'field'         => DecimalDataField::create("decimal?", 4, 1, true, true),
        'expectedValue' => null
      ],
      // "nullable DateTime" =>  [
      //   'field' => new DateTimeDataField("DateTime?", DateTimeFormat::DateAndTime, true),
      //   'expectedValue' => null,
      // ],

      "bool" => [
        'field'         => DataField::createFromType("bool", 'boolean'),
        'expectedValue' => true
      ],
      // "nullable bool" => [
      //   'field'         => DataField::createFromType("bool?", 'bool?'),
      //   'expectedValue' => new bool?(true)
      // ],
    ];
  }

  /**
   * [testTypeWritesAndReadsEndToEnd description]
   */
  public function testTypeWritesAndReadsEndToEnd(): void {
    static::createTestData();

    foreach(static::$nameToData as $key => $value) {
      $this->Type_writes_and_reads_end_to_end($key);
    }
  }

  /**
   * [Type_writes_and_reads_end_to_end description]
   * @param string $name [description]
   */
  protected function Type_writes_and_reads_end_to_end(string $name): void
  {
    $input = static::$nameToData[$name];
    $actual = $this->WriteReadSingle($input['field'], $input['expectedValue']);
    $equal = null;
    if ($input['expectedValue'] === null && $actual === null)
    {
      $equal = true;
    }
    // TODO: Tests using arrays
    // else if (actual.GetType().IsArrayOf<byte>() && input.expectedValue != null)
    // {
    //   equal = ((byte[]) actual).SequenceEqual((byte[]) input.expectedValue);
    // }
    else
    {
      $equal = ($actual == $input['expectedValue']);
    }

    $expectedValueFormatted = print_r($input['expectedValue'], true);
    $actualFormatted = print_r($actual, true);
    $fieldFormatted = print_r($input['field'], true);

    if($input['delta'] ?? false) {
      $this->assertEqualsWithDelta($input['expectedValue'], $actual, $input['delta'], "{$name} | expected: [ {$expectedValueFormatted} ], actual: [ {$actualFormatted} ], schema element: {$fieldFormatted}");
    } else {
      $this->assertTrue($equal, "{$name} | expected: [ {$expectedValueFormatted} ], actual: [ {$actualFormatted} ], schema element: {$fieldFormatted}");
    }
  }
}
