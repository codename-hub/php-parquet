<?php
declare(strict_types=1);
namespace codename\parquet\tests\values\primitives;

use codename\parquet\format\SchemaElement;
use codename\parquet\tests\TestBase;

use codename\parquet\values\primitives\BigDecimal;

final class BigDecimalTest extends TestBase
{
  public function testValidButMassiveBigDecimal(): void {
    $decimalAsString = "83086059037282.54";

    // Fake element for using the helper methods
    $schema = new SchemaElement();
    $schema->precision = 38;
    $schema->scale = 16;

    $bd = BigDecimal::DecimalToBinary($decimalAsString, $schema->precision, $schema->scale);
   
    $byteString = pack("C*", ...$bd);
    $reverse = BigDecimal::BinaryDataToDecimal($byteString, $schema);

    $reverseWithoutZeros = rtrim($reverse,'0'); // strip zeros
    $this->assertEquals($decimalAsString, $reverseWithoutZeros);
  }

  public function testValidButMassiveBigDecimalExactPrecision(): void {
    $decimalAsString = "83086059037282.54154657613687";

    // Fake element for using the helper methods
    $schema = new SchemaElement();
    $schema->precision = 38;
    $schema->scale = 16;

    $bd = BigDecimal::DecimalToBinary($decimalAsString, $schema->precision, $schema->scale);
   
    $byteString = pack("C*", ...$bd);
    $reverse = BigDecimal::BinaryDataToDecimal($byteString, $schema);

    $reverseWithoutZeros = rtrim($reverse,'0'); // strip zeros
    $this->assertEquals($decimalAsString, $reverseWithoutZeros);
  }
}
