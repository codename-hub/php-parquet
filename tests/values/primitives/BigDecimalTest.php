<?php
declare(strict_types=1);
namespace jocoon\parquet\tests\values\primitives;

use jocoon\parquet\tests\TestBase;

use jocoon\parquet\values\primitives\BigDecimal;

final class BigDecimalTest extends TestBase
{
  /**
   * [testValidButMassiveBigDecimal description]
   */
  public function testValidButMassiveBigDecimal(): void {
    $bd = BigDecimal::DecimalToBinary("83086059037282.54", 38, 16);
  }
}
