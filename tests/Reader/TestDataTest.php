<?php
declare(strict_types=1);
namespace jocoon\parquet\tests\Reader;

use DateTimeImmutable;
use DateTimeInterface;

use jocoon\parquet\tests\TestBase;

class TestDataTest extends ParquetCsvComparison
{
  /**
   * [testAllTypesPlainNoCompression description]
   */
  public function testAllTypesPlainNoCompression(): void {
    $this->CompareFiles('types/alltypes', 'plain', true, [
      'integer',
      'boolean',
      'integer',
      'integer',
      'integer',
      'integer', // long
      'float', // note:  might return double, internally, see https://www.php.net/manual/de/function.gettype
      'double',
      'string',
      'string',
      \DateTimeImmutable::class,
    ]);
  }

  /**
   * [testAllTypesGzipCompression description]
   */
  public function testAllTypesGzipCompression(): void {
    $this->CompareFiles('types/alltypes', 'gzip', true, [
      'integer',
      'boolean',
      'integer',
      'integer',
      'integer',
      'integer', // long
      'float', // note:  might return double, internally, see https://www.php.net/manual/de/function.gettype
      'double',
      'string',
      'string',
      \DateTimeImmutable::class,
    ]);
  }

  /**
   * [testAllTypesSnappyCompression description]
   */
  public function testAllTypesSnappyCompression(): void {
    if(!extension_loaded('snappy')) {
      static::markTestSkipped('ext-snappy unavailable');
    }
    $this->CompareFiles('types/alltypes', 'snappy', true, [
      'integer',
      'boolean',
      'integer',
      'integer',
      'integer',
      'integer', // long
      'float', // note:  might return double, internally, see https://www.php.net/manual/de/function.gettype
      'double',
      'string',
      'string',
      \DateTimeImmutable::class,
    ]);
  }

  // /**
  //  * [testAllTypesPlainNoCompressionByteArrays description]
  //  */
  // public function testAllTypesPlainNoCompressionByteArrays(): void {
  //   $this->CompareFiles('types/alltypes', 'plain', false, [
  //     'integer',
  //     'boolean',
  //     'integer',
  //     'integer',
  //     'integer',
  //     'integer', // long
  //     'float', // note:  might return double, internally, see https://www.php.net/manual/de/function.gettype
  //     'double',
  //     'string',
  //     'string',
  //     \DateTimeImmutable::class,
  //   ]);
  // }

  /**
   * [testAllTypesDictionaryNoCompression description]
   */
  public function testAllTypesDictionaryNoCompression(): void {
    $this->CompareFiles('types/alltypes_dictionary', 'plain-spark21', true, [
      'integer',
      'boolean',
      'integer',
      'integer',
      'integer',
      'integer', // long
      'float', // note:  might return double, internally, see https://www.php.net/manual/de/function.gettype
      'double',
      'string',
      'string',
      \DateTimeImmutable::class,
    ]);
  }


  // ...

  /**
   * [testPostcodesSampleNoCompression description]
   */
  public function testPostcodesSampleNoCompression(): void {
    $this->CompareFiles('postcodes', 'plain', true, [
      'string',   //Postcode
      'string',   //
      'double',
      'double',
      'integer',     //Easting
      'integer',     //Northing
      'string',
      'string',
      'string',
      'string',
      'string',
      'string',
      'string',
      'string',
      'string',   //Constituency
      \DateTimeImmutable::class,
      \DateTimeImmutable::class,
      'string',   //Parish
      'string',   //NationalPark
      'integer',     //Population
      'integer',
      'string',
      'string',
      'string',
      'string',
      'string',
      'integer',
      'integer',
      'string'
    ]);
  }

}
