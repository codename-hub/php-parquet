<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

class BadDataTest extends TestBase
{
  /**
   * [testParquet1481 description]
   */
  public function testParquet1481(): void
  {
    $this->expectException(\Exception::class);
    $reader = new ParquetReader($this->openTestFile('bad/PARQUET-1481.parquet'));
  }
}
