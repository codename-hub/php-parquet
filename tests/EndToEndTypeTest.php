<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;

use codename\parquet\data\DataType;
use codename\parquet\data\DataField;
use codename\parquet\data\DateTimeFormat;
use codename\parquet\data\DecimalDataField;
use codename\parquet\data\DateTimeDataField;

final class EndToEndTypeTest extends TestBase
{
  /**
   * Datasets as a dataProvider
   * to be executed by methods in this class.
   *
   * @return array [description]
   */
  public function endToEndTestDataProvider(): array {
    $dataset = [
      "plain string" => [
        'field'           => DataField::createFromType("plain string", 'string'),
        'expectedValue'   => "plain string"
      ],
      "unicode string" => [
        'field'           => DataField::createFromType("unicode string", 'string'),
        'expectedValue'   => "L'Oréal Paris"
      ],
      "byte array" => [
        'field'         => DataField::createFromType("byte array", 'array', 'byte'),
        'expectedValue' => array_values(unpack('C*', "raw byte string"))
      ],
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
        'expectedValue' => 0x00 // byte.MinValue
      ],
      "byte max value" => [
        'field'         => DataField::createFromType("byte", 'byte'),
        'expectedValue' => 0xFF // 0xFF
      ],

      // "signed byte min value" => [
      //   'field'         => DataField::createFromType("sbyte", 'sbyte'),
      //   'expectedValue' => sbyte.MinValue
      // ],
      // "signed byte max value" => [
      //   'field'         => DataField::createFromType("sbyte", 'sbyte'),
      //   'expectedValue' => sbyte.MaxValue
      // ],
      //
      "short min value" => [
        'field'         => new DataField("short", DataType::Short),
        'expectedValue' => -32768
      ],
      "short max value" => [
        'field'         => new DataField("short", DataType::Short),
        'expectedValue' => 32767
      ],

      "short min value underflow" => [
        'field'         => new DataField("short", DataType::Short),
        'expectedValue' => -32768 - 1,
        'expectExceptionMessage' => 'Value out of Int16 bounds'
      ],
      "short max value overflow" => [
        'field'         => new DataField("short", DataType::Short),
        'expectedValue' => 32767 + 1,
        'expectExceptionMessage' => 'Value out of Int16 bounds'
      ],

      "unsigned short min value" => [
        'field'         => new DataField("ushort", DataType::UnsignedShort), // DataField::createFromType("ushort", 'ushort'),
        'expectedValue' => 0 // ushort.MinValue
      ],
      "unsigned short max value" => [
        'field'         => new DataField("ushort", DataType::UnsignedShort),
        'expectedValue' => 65535
      ],
      "unsigned short min value underflow" => [
        'field'         => new DataField("ushort", DataType::UnsignedShort), // DataField::createFromType("ushort", 'ushort'),
        'expectedValue' => 0 - 1, // ushort.MinValue
        'expectExceptionMessage' => 'Value out of UInt16 bounds'
      ],
      "unsigned short max value overflow" => [
        'field'         => new DataField("ushort", DataType::UnsignedShort),
        'expectedValue' => 65535 + 1,
        'expectExceptionMessage' => 'Value out of UInt16 bounds'
      ],

      "int min value" => [
        'field'         => new DataField("int", DataType::Int32), // DataField::createFromType("int", 'integer'),
        'expectedValue' => -2147483648
      ],
      "int max value" => [
        'field'         => new DataField("int", DataType::Int32),
        'expectedValue' => 2147483647
      ],
      "int min value underflow" => [
        'field'         => new DataField("int", DataType::Int32), // DataField::createFromType("int", 'integer'),
        'expectedValue' => -2147483648 - 1,
        'expectExceptionMessage' => 'Value out of Int32 bounds'
      ],
      "int max value overflow" => [
        'field'         => new DataField("int", DataType::Int32),
        'expectedValue' => 2147483647 + 1,
        'expectExceptionMessage' => 'Value out of Int32 bounds'
      ],

      "unsigned int min value" => [
        'field'         => new DataField("int", DataType::UnsignedInt32), // DataField::createFromType("int", 'integer'),
        'expectedValue' => 0
      ],
      "unsigned int max value" => [
        'field'         => new DataField("int", DataType::UnsignedInt32),
        'expectedValue' => 4294967295
      ],
      "unsigned int min value underflow" => [
        'field'         => new DataField("int", DataType::UnsignedInt32), // DataField::createFromType("int", 'integer'),
        'expectedValue' => 0 - 1,
        'expectExceptionMessage' => 'Value out of UInt32 bounds'
      ],
      "unsigned int max value overflow" => [
        'field'         => new DataField("int", DataType::UnsignedInt32),
        'expectedValue' => 4294967295 + 1,
        'expectExceptionMessage' => 'Value out of UInt32 bounds'
      ],

      "long min value" => [
        'field'         => new DataField("long", DataType::Int64), // DataField::createFromType("int", 'integer'),
        'expectedValue' => -9223372036854775808
      ],
      "long max value" => [
        'field'         => new DataField("long", DataType::Int64), // DataField::createFromType("int", 'integer'),
        'expectedValue' => 9223372036854775807
      ],

      //
      // Unsigned Longs are not supported due to the nature of php
      // you may use a regular, signed int64
      //
      // "unsigned long min value" => [
      //   'field'         => new DataField("long", DataType::UnsignedInt64), // DataField::createFromType("int", 'integer'),
      //   'expectedValue' => 0
      // ],
      // "unsigned long max value" => [
      //   'field'         => new DataField("long", DataType::UnsignedInt64), // DataField::createFromType("int", 'integer'),
      //   'expectedValue' => 18446744073709551615
      // ],


      // "unsigned long max value near bound" => [
      //   'field'         => new DataField("long", DataType::Int64), // DataField::createFromType("int", 'integer'),
      //   'expectedValue' => 18446744073709551615 - 1
      // ],

      // ["long min value"] = (new DataField<long>("long"), long.MinValue),
      // ["long max value"] = (new DataField<long>("long"), long.MaxValue),
      // ["unsigned long min value"] = (new DataField<ulong>("ulong"), ulong.MinValue),
      // ["unsigned long max value"] = (new DataField<ulong>("ulong"), ulong.MaxValue),

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

    $dataprovider = [];
    foreach($dataset as $name => $data) {
      $dataprovider[] = [ $name, $data ];
    }
    return $dataprovider;
  }

  /**
   * To demonstrate array equality comparison
   */
  public function testArrayEqualityComparison() : void {
    $expected = [ 1, 2, 3 ];
    $this->assertTrue([ 1, 2, 3 ] === $expected);
    $this->assertFalse([ 3, 2, 1 ] === $expected);
    $this->assertFalse([ 2, 1 ] === $expected);
    $this->assertFalse([ ] === $expected);
    $this->assertFalse('123' === $expected);
  }

  /**
   * This method executes all the tests described in the dataProvider above.
   *
   * @dataProvider endToEndTestDataProvider
   * @param string  $name
   * @param array   $data
   */
  public function testTypeWritesAndReadsEndToEnd(string $name, array $data): void
  {
    $input = $data;

    $expectExceptionMessage = $input['expectExceptionMessage'] ?? null;
    if($expectExceptionMessage) {
      $this->expectExceptionMessage($expectExceptionMessage);
    }

    $actual = $this->WriteReadSingle($input['field'], $input['expectedValue']);
    $equal = null;
    if ($input['expectedValue'] === null && $actual === null)
    {
      $equal = true;
    }
    else if(is_array($input['expectedValue'])) {
      // See testArrayEqualityComparison()
      $equal = ($actual === $input['expectedValue']);
    }
    else
    {
      // NOTE: no identity (===) comparison, as there might be objects
      // which are never identical
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

    // For some hard-debugging purposes, comment-in
    // if($input['debug'] ?? false) {
    //   print_r([
    //     'input'   => $input,
    //     'actual'  => $actual
    //   ]);
    // }
  }
}
