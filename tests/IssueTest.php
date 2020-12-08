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

  /**
   * @see https://github.com/aloneguid/parquet-dotnet/issues/87
   */
  public function testParquetDotnetIssue87(): void
  {
    $this->markTestIncomplete('Marked test as incomplete, need more information whether this really is an issue or not.');

    $reader = new ParquetReader($this->openTestFile('issues/parquet-dotnet_87_running_numbers_spark.gz.parquet'));

    $data = $reader->ReadEntireRowGroup();
    $expectedRows = 10000;

    $numbers = $data[0]->getData();
    $txt = $data[1]->getData();

    $this->assertCount($expectedRows, $numbers);
    $this->assertCount($expectedRows, $txt);

    for ($i=0; $i < $expectedRows; $i++) {
      $num = $numbers[$i];
      if($num % 100 === 0) {
        $this->assertNull($txt[$i]);
      } else {
        $txtIndex = $num % 10;
        $expected = implode('', array_fill(0, 1000, $txtIndex . ', '));
        $actual = $txt[$i];

        $this->assertEquals($expected, $actual, "Text doesn't match at Row {$i}");
      }
    }
  }
}
