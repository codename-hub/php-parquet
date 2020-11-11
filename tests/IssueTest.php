<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

class IssueTest extends TestBase
{
  /**
   * Tests a specific issue of parquet-dotnet
   * to be or not to be present in our package
   * @see https://github.com/aloneguid/parquet-dotnet/issues/81
   */
  public function testParquetDotnetIssue81(): void
  {
    $reader = new ParquetReader($this->openTestFile('issues/parquet-dotnet_81_floats.parquet'));
    $dataColumns = $reader->ReadEntireRowGroup();
    $values = $dataColumns[0]->getData();

    $this->assertCount(270000, $values);

    foreach($values as $i => $v) {
      $this->assertEquals($i+1, $v);
    }
  }
}
