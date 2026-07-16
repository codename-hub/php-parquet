<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;

final class VariousTest extends TestBase {

  /**
   * [testParquetTestingSingleNan description]
   * @requires extension snappy
   */
  public function testParquetTestingSingleNan(): void {
    $reader = new ParquetReader($this->openTestFile('single_nan.parquet'));
    $columns = $reader->ReadEntireRowGroup();
    $this->assertEquals(1, $reader->getThriftMetadata()->num_rows);
    $this->assertEquals(1, $columns[0]->statistics->nullCount);
    $this->assertEquals([ null ], $columns[0]->getData());
  }

  /**
   * [testParquet1402Arrow5322 description]
   * @requires extension snappy
   */
  public function testParquet1402Arrow5322(): void {
    $this->expectNotToPerformAssertions();
    $reader = new ParquetReader($this->openTestFile('dict-page-offset-zero.parquet'));
    $columns = $reader->ReadEntireRowGroup();
  }

  /**
   * [testRleDict description]
   */
  public function testRleDict(): void {
    //
    // NOTE: this file is a little bit strange as the name implies
    // fastparquet returns only column "id", while pyarrow reads also "phoneNumbers"
    // (and some more struct fields)
    //
    $reader = new ParquetReader($this->openTestFile('repeated_no_annotation.parquet'));
    $columns = $reader->ReadEntireRowGroup();
    $this->assertEquals([ 1.0, 2.0, 3.0, 4.0, 5.0, 6.0 ], $columns[0]->getData());
  }

}
