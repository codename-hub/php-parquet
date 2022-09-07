<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetOptions;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;

use codename\parquet\helper\ParquetDataWriter;
use codename\parquet\helper\ParquetDataIterator;

class ParquetDataWriterTest extends TestBase
{
  /**
   * [testSimple description]
   */
  public function testSimple(): void {
    $schema = new Schema([
      DataField::createFromType('id', 'integer'),
      DataField::createFromType('name', 'string'),
    ]);

    $handle = fopen('php://memory', 'r+');
    $instance = new ParquetDataWriter($handle, $schema);

    for ($i=0; $i < 15001; $i++) {
      $instance->put([
        'id'    => $i,
        'name'  => 'id#'.$i,
      ]);
    }
    $instance->finish();

    fseek($handle, 0, SEEK_SET);

    // Make sure we really wrote 4 row groups
    // 5000 + 5000 + 5000 + 1
    $reader = new ParquetReader($handle);
    $this->assertEquals(4, $reader->getRowGroupCount());

    fseek($handle, 0, SEEK_SET);

    // Re-read data
    $iterator = ParquetDataIterator::fromHandle($handle);

    $cnt = 0;
    foreach($iterator as $i => $data) {
      $this->assertEquals($i, $data['id']);
      $this->assertEquals('id#'.$i, $data['name']);
      $cnt++;
    }
    $this->assertEquals(15001, $cnt);
  }

  /**
   * Tests changing desired/preferred row group size
   * during write/data accumulation/buffer filling
   */
  public function testVaryingRowGroupSize(): void {
    $schema = new Schema([
      DataField::createFromType('id', 'integer'),
      DataField::createFromType('name', 'string'),
    ]);

    $handle = fopen('php://memory', 'r+');
    $instance = new ParquetDataWriter($handle, $schema);
    $instance->setRowGroupSize(20000);

    for ($i=0; $i < 15001; $i++) {
      $instance->put([
        'id'    => $i,
        'name'  => 'id#'.$i,
      ]);

      switch($i) {
        case 3000:
          // single batch of 3000 (0-2999)
          $instance->setRowGroupSize(3000);
          break;
        case 5000:
          // two batches of 2000 (3000-4999, 5000-6999)
          $instance->setRowGroupSize(2000);
          break;
        case 7000:
          // three batches of 1000 (7000-7999, 8000-8999, 9000-9999)
          $instance->setRowGroupSize(1000);
          break;
        case 10000:
          // last remaining batch of 5001 (10000-15001)
          $instance->setRowGroupSize(10000);
          break;
      }
    }

    // $instance->setRowGroupSize(3000);
    $instance->finish();

    fseek($handle, 0, SEEK_SET);

    // Make sure we really wrote 7 row groups
    $reader = new ParquetReader($handle);
    $this->assertEquals(7, $reader->getRowGroupCount());

    // compare row group sizes
    $rowCounts = [];
    for ($i=0; $i < $reader->getRowGroupCount(); $i++) {
      $rg = $reader->OpenRowGroupReader($i);
      $rowCounts[] = $rg->getRowCount();
    }
    $this->assertEquals([3000,2000,2000,1000,1000,1000,5001], $rowCounts);

    fseek($handle, 0, SEEK_SET);

    // Re-read data
    $iterator = ParquetDataIterator::fromHandle($handle);

    $cnt = 0;
    foreach($iterator as $i => $data) {
      $this->assertEquals($i, $data['id']);
      $this->assertEquals('id#'.$i, $data['name']);
      $cnt++;
    }
    $this->assertEquals(15001, $cnt);
  }

  /**
   * [dataProviderTestFiles description]
   * @return array [description]
   */
  public function dataProviderTestFiles(): array {
    return [
      [ 'datetime_other_system.parquet' ],
      [ 'emptycolumn.parquet', [ 'snappy' ] ],
      [ 'customer.impala.parquet' ],
      [ 'special/multi_data_page.parquet' ],
      [ 'postcodes.plain.parquet' ],
      [ 'postcodes.datapageV2.pagesize256.parquet' ],
      [ 'single_nan.parquet', [ 'snappy' ] ],
      [ 'special/multi_data_page.parquet' ],
      [ 'special/multi_page_bit_packed_near_page_border.parquet', [ 'snappy' ] ],
      [ 'special/multi_page_dictionary_with_nulls.parquet' ],
      [ 'real/nation.plain.parquet' ],
      [ 'nested_lists.snappy.parquet', [ 'snappy' ] ],
      [ 'nested_maps.snappy.parquet', [ 'snappy' ] ],
      [ 'nested_structs.parquet', [ 'snappy' ] ],
      [ 'repeated_no_annotation.parquet', [ 'snappy' ] ]
    ];
  }

  /**
   * @dataProvider dataProviderTestFiles
   *
   * @param  string   $file               [description]
   * @param  string[] $flags
   */
  public function testReadWrite(string $file, $flags = null) {

    // Just to workaround unavailable snappy ext for some files
    if($flags) {
      if(in_array('snappy', $flags)) {
        if(!extension_loaded('snappy')) {
          $this->markTestSkipped('ext-snappy unavailable');
        }
      }
    }

    $options = new ParquetOptions;
    $options->TreatByteArrayAsString = True;

    $iterator = ParquetDataIterator::fromHandle($this->openTestFile($file), $options);

    $handle = fopen('php://memory', 'r+');
    $writer = new ParquetDataWriter($handle, $iterator->getUnderlyingParquetReader()->schema, $options);

    foreach($iterator as $i => $dataset) {
      $writer->put($dataset);
    }

    $writer->finish();
    fseek($handle, 0, SEEK_SET);

    $compareIterator = ParquetDataIterator::fromHandle($handle, $options);

    $this->assertEquals($iterator->count(), $compareIterator->count());

    foreach($iterator as $i => $dataset) {
      $compareIterator->next();
      if ($compareIterator->valid()) {
        $this->assertEquals($dataset, $compareIterator->current());
      } else {
        $this->fail("Invalid offset in compareIterator #".$i);
      }
    }
  }
}
