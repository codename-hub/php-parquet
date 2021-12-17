<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;

use codename\parquet\helper\ParquetDataIterator;
use codename\parquet\helper\DataColumnsToArrayConverter;

class IteratorTest extends TestBase
{
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
   * @param string      $file   [description]
   * @param array|null  $flags  [description]
   */
  public function testParquetDataIterator(string $file, ?array $flags = null): void {

    // Just to workaround unavailable snappy ext for some files
    if($flags) {
      if(in_array('snappy', $flags)) {
        if(!extension_loaded('snappy')) {
          $this->markTestSkipped('ext-snappy unavailable');
        }
      }
    }

    $options = new \codename\parquet\ParquetOptions();
    // By default, use this one to reduce memory usage for those tests
    $options->TreatByteArrayAsString = true;

    // Init a new iterator
    $iterator = ParquetDataIterator::fromHandle($this->openTestFile($file), $options);

    // Open the same file a second time for comparison
    $reader = new ParquetReader($this->openTestFile($file), $options);

    // // make sure count() implementation works
    // $this->assertCount($reader->getThriftMetadata()->num_rows, $iterator);

    // WARNING: $reader->getThriftMetadata()->num_rows might be wrong
    // as somebody has written wrong information into the file
    $calculatedRowCount = 0;

    $compareData = [];

    for ($i=0; $i < $reader->getRowGroupCount(); $i++) {
      $rgr = $reader->OpenRowGroupReader($i);
      $calculatedRowCount += $rgr->getRowCount(); // manually count
      $dataColumns = $reader->ReadEntireRowGroup($i);
      $converter = new DataColumnsToArrayConverter($reader->schema, $dataColumns);
      $compareData = array_merge($compareData, $converter->toArray());
    }


    // manually call rewind
    $iterator->rewind();

    foreach($compareData as $index => $value) {
      $this->assertTrue($iterator->valid());
      $this->assertEquals($value, $iterator->current());
      $iterator->next();
    }

    // make sure the last entry is invalid
    // as we finished iterating
    $this->assertFalse($iterator->valid());

    // second iteration run
    $iterator->rewind();
    foreach($compareData as $index => $value) {
      $this->assertTrue($iterator->valid());
      $this->assertEquals($value, $iterator->current());
      $iterator->next();
    }

    // this is the actual comparison we have to perform
    // to make sure we have read the exact amount of entries
    $this->assertCount($calculatedRowCount, $compareData);

    // in most cases, this works, but it's not reliable
    // (e.g. repeated_no_annotation.parquet does NOT have a valid num_rows)
    // $this->assertCount($reader->getThriftMetadata()->num_rows, $compareData);
    // The iterator internally uses the correct method to do it.
    $this->assertCount($calculatedRowCount, $iterator);
  }
}
