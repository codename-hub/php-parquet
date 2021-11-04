<?php
declare(strict_types=1);
namespace codename\parquet\tests\values\primitives;

use codename\parquet\tests\TestBase;

use codename\parquet\values\primitives\BigDecimal;

final class BigDecimalTest extends TestBase
{
  /**
   * [testValidButMassiveBigDecimal description]
   */
  public function testValidButMassiveBigDecimal(): void {
    $bd = BigDecimal::DecimalToBinary("83086059037282.54", 38, 16);
  }
}
