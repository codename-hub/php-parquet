<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetWriter;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;
use codename\parquet\data\StructField;

use codename\parquet\helper\ParquetDataIterator;
use codename\parquet\helper\ArrayToDataColumnsConverter;
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
      if($index !== $iterator->key()) {
        $this->fail("Index/key mismatch {$index} != {$iterator->key()}");
      }
      if(!$iterator->valid()) {
        $this->fail("Iterator returned valid == false at index {$index}");
      }
      // We need to perform a deep equality comparison
      // if($value !== $iterator->current()) {
      //   $this->fail("Dataset not equal at index {$index}");
      // }
      $this->assertEquals($value, $iterator->current());
      $iterator->next();
    }

    // make sure the last entry is invalid
    // as we finished iterating
    $this->assertFalse($iterator->valid());

    // second iteration run
    $iterator->rewind();
    foreach($compareData as $index => $value) {
      if($index !== $iterator->key()) {
        $this->fail("Index/key mismatch {$index} != {$iterator->key()}");
      }
      if(!$iterator->valid()) {
        $this->fail("Iterator returned valid == false at index {$index}");
      }
      // We need to perform a deep equality comparison
      // if($value !== $iterator->current()) {
      //   $this->fail("Dataset not equal at index {$index}");
      // }
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

  /**
   * Tests behaviour equality of fromHandle and fromFile
   */
  public function testIteratorFromFile(): void {
    $fromHandleIterator = ParquetDataIterator::fromHandle($this->openTestFile('postcodes.plain.parquet'));
    $fromFileIterator = ParquetDataIterator::fromFile(__DIR__.'/data/postcodes.plain.parquet');

    $this->assertEquals($fromHandleIterator->count(), $fromFileIterator->count());

    $fromFileIterator->rewind();
    foreach($fromHandleIterator as $index => $value) {

      if($index !== $fromFileIterator->key()) {
        $this->fail("Index/key mismatch {$index} != {$fromFileIterator->key()}");
      }
      if(!$fromFileIterator->valid()) {
        $this->fail("Iterator returned valid == false at index {$index}");
      }
      $this->assertEquals($value, $fromFileIterator->current());
      $fromFileIterator->next();
    }
    $this->assertFalse($fromHandleIterator->valid());
    $this->assertFalse($fromFileIterator->valid());
  }

  /**
   * [testDeeperPathmapUsingDataColumnIterable description]
   */
  public function testDeeperPathmapUsingDataColumnIterable(): void {
    $schema = new Schema([
      //
      // By using a struct with nested elements
      // we implicitly have a deeper DL pathmap internally
      // As we're using ParquetDataIterator, this implicitly leverages
      // DataColumnIterable.
      // Therefore, this covers
      // DataColumnIterable, non-repeated field and a deeper DL/nesting level
      //
      StructField::createWithFieldArray('struct1', [
          DataField::createFromType('id', 'integer'),
          DataField::createFromType('name', 'string'),
        ],
        true // nullable struct
      )
    ]);

    $data = [
      // entry 0
      [
        'struct1' => [
          'id' => 1,
          'name' => 'abc'
        ]
      ],
      // entry 1
      [
        'struct1' => [
          'id' => 2,
          'name' => 'def'
        ]
      ]
    ];

    // write stuff
    $dataColumnsConverter = new ArrayToDataColumnsConverter($schema, $data);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    // Read again using ParquetDataIterator
    // which uses DataColumnIterable internally
    $fromHandleIterator = ParquetDataIterator::fromHandle($ms);

    $compareData = [];
    foreach($fromHandleIterator as $index => $value) {
      $compareData[$index] = $value;
    }

    $this->assertEquals($data, $compareData);
  }

  /**
   * Tests reading multiple row groups
   * by creating a sample case first
   */
  public function testReadMultipleRowGroups(): void {

    //write a single file having 3 row groups and two fields
    $id = DataField::createFromType('id', 'integer');
    $name = DataField::createFromType('name', 'string');
    $ms = fopen('php://memory', 'r+');

    $writer = new ParquetWriter(new Schema([$id, $name]), $ms);

    $rg = $writer->createRowGroup();
    $rg->WriteColumn(new DataColumn($id, [ 1 ]));
    $rg->WriteColumn(new DataColumn($name, [ 'abc' ]));
    $rg->finish();

    $rg = $writer->createRowGroup();
    $rg->WriteColumn(new DataColumn($id, [ 2 ]));
    $rg->WriteColumn(new DataColumn($name, [ 'def' ]));
    $rg->finish();

    $rg = $writer->createRowGroup();
    $rg->WriteColumn(new DataColumn($id, [ 3 ]));
    $rg->WriteColumn(new DataColumn($name, [ 'ghi' ]));
    $rg->finish();

    $writer->finish();

    fseek($ms, 0);

    $iterator = ParquetDataIterator::fromHandle($ms);

    $result = [];
    foreach($iterator as $index => $value) {
      $result[$index] = $value;
    }

    $this->assertEquals([
      [ 'id' => 1, 'name' => 'abc' ],
      [ 'id' => 2, 'name' => 'def' ],
      [ 'id' => 3, 'name' => 'ghi' ],
    ], $result);
  }
}
