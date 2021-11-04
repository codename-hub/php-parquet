<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;

/**
 * This test class tests Datapage V2 (and some format 2.0-specs)
 * and also tries to test RLE-related specialties
 */
final class DatapageV2Test extends TestBase
{
  /**
   * provides files to be used for testDatapageV2TestFile
   * @return array [description]
   */
  public function postcodesFilesDataProvider(): array {
    return [
      //
      // Python script base:
      //
      //  import pyarrow.parquet as pq
      //  table = pq.read_table('postcodes.plain.parquet')
      //
      [
        // DATA_PAGE_V2 + v2 format specs, no dictionary usage
        // pq.write_table(table, 'postcodes.datapageV2.schemaV2.coercedTS.plain.parquet', version='2.0', data_page_version='2.0', coerce_timestamps='ms', compression='NONE', use_dictionary=False)
        'postcodes.datapageV2.schemaV2.coercedTS.plain.parquet'
      ],
      [
        // DATA_PAGE_V2 + v2 format specs, no dictionary usage, GZIP compression
        // pq.write_table(table, 'postcodes.datapageV2.schemaV2.coercedTS.gzip.parquet', version='2.0', data_page_version='2.0', coerce_timestamps='ms', compression='GZIP', use_dictionary=False)
        'postcodes.datapageV2.schemaV2.coercedTS.gzip.parquet'
      ],
      [
        // DATA_PAGE_V2 + v2 format specs, dictionary usage, fixed data_page_size of 256 bytes
        // pq.write_table(table, 'postcodes.datapageV2.pagesize256.parquet', version='2.0', data_page_version='2.0', coerce_timestamps='ms', compression='NONE', use_dictionary=True, data_page_size=256)
        'postcodes.datapageV2.pagesize256.parquet'
      ],
      [
        // DATA_PAGE_V2 + v2 format specs, dictionary usage, fixed data_page_size of 64 bytes
        // pq.write_table(table, 'postcodes.datapageV2.pagesize64.parquet', version='2.0', data_page_version='2.0', coerce_timestamps='ms', compression='NONE', use_dictionary=True, data_page_size=64)
        'postcodes.datapageV2.pagesize64.parquet'
      ],
      [
        // DATA_PAGE_V2 + v2 format specs, dictionary usage
        // pq.write_table(table, 'postcodes.datapageV2.schemaV2.rle.coercedTS.gzip.parquet', version='2.0', data_page_version='2.0', coerce_timestamps='ms', compression='GZIP', use_dictionary=True)
        'postcodes.datapageV2.schemaV2.rle.coercedTS.gzip.parquet'
      ],
    ];
  }

  /**
   * @dataProvider postcodesFilesDataProvider
   * @param  string $file
   */
  public function testDatapageV2TestFile(string $file): void {
    // Original file (V1)
    $rOriginal = new ParquetReader($this->openTestFile('postcodes.plain.parquet'));
    $columnsOriginal = $rOriginal->ReadEntireRowGroup();

    $dataOriginal = [];
    foreach($columnsOriginal as $col) {
      $dataOriginal[$col->getField()->name] = $col->getData();
    }

    // Load a file to test against
    $r = new ParquetReader($this->openTestFile($file));
    $columns = $r->ReadEntireRowGroup();

    $data = [];
    foreach($columns as $col) {
      $data[$col->getField()->name] = $col->getData();
    }

    $this->assertEquals($dataOriginal, $data);
  }
}
