<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetException;
use Exception;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetWriter;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;

final class MetadataTest extends TestBase
{
  public function getGoodMetadataProvider() {
    return [
      [['author' => 'santa', 'date' => '2020-01-01',    ]], // regular string value
      [['author' => '',      'date' => '2020-01-01',    ]], // empty value
      [['author' => null,    'date' => '2020-01-01',    ]], // null value
      [['author' => ' ',     'date' => '2020-01-01',    ]], // whitespace value
      [['theOnlyKey' => null]], // single null key
      [['theOnlyKey' => '']], // single empty string KV
      [['theOnlyKey' => ' ']], // single whitespace string KV
      [['' => 'ABC']], // empty string key
      [[' ' => ' ']], // whitespace for KV
      [['' => '']], // empty string for KV
    ];
  }

  /**
   * Test Write/Read custom metadata
   * @dataProvider getGoodMetadataProvider
   */
  public function testStoreMetadata($metadata): void
  {
    $ms = fopen('php://memory', 'r+');
    $id = DataField::createFromType('id', 'integer');

    //write
    $writer = new ParquetWriter(new Schema([$id]), $ms);
    $writer->setCustomMetadata($metadata);

    $rg = $writer->CreateRowGroup();
    $rg->WriteColumn(new DataColumn($id, [ 1, 2, 3, 4 ]));
    $rg->finish();
    $writer->finish();

    //read back
    $reader = new ParquetReader($ms);
    $this->assertEquals(4, $reader->getThriftMetadata()->num_rows);

    $this->assertEquals($metadata, $reader->getCustomMetadata());
  }

  public function getBadMetadataProvider(): array{
    return [
      [['author' => true,    'date' => '2020-01-01',    ]], // boolean value
      [['author' => 123,     'date' => '2020-01-01',    ]], // number value
      [['author' => 'test',  'date' => new \DateTime(), ]], // object value
    ];
  }

  /**
   * Test Set bad custom metadata
   * @dataProvider getBadMetadataProvider
   */
  public function testFailsMetadata($metadata): void
  {
    $ms = fopen('php://memory', 'r+');
    $id = DataField::createFromType('id', 'integer');

    $this->expectException(ParquetException::class);
    $this->expectExceptionMessage('Invalid KeyValue pair provided in CustomMetadata');

    (new ParquetWriter(new Schema([$id]), $ms))->setCustomMetadata($metadata);
  }

  /**
   * Reading metadata from a file created in pyarrow
   */
  public function testReadMetadata(): void {
    $reader = new ParquetReader($this->openTestFile('movies_pyarrow.parquet'));
    $metadata = $reader->getCustomMetadata();
    $this->assertArrayHasKey('great_music', $metadata);
    $this->assertEquals('reggaeton', $metadata['great_music']);
  }

}
