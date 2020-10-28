<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

use jocoon\parquet\data\DataField;

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
      "simple DateTime" => [
        'field'           => DataField::createFromType('datetime', \DateTimeImmutable::class),
        'expectedValue'   => new \DateTimeImmutable('now')
      ]
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
    $this->assertTrue($equal, "{$name} | expected: [ {$expectedValueFormatted} ], actual: [ {$actualFormatted} ], schema element: {$fieldFormatted}");
  }
}
