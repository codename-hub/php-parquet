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
  /**
   * Test Write/Read custom metadata
   */
  public function testStoreMetadata(): void
  {
    $ms = fopen('php://memory', 'r+');

    $id = DataField::createFromType('id', 'integer');
    $metadata = [
      'author'=>'santa',
      'date'=>'2020-01-01',
    ];

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

    $this->assertEquals($metadata,   $reader->getCustomMetadata());
  }

  public function getBadMetadataProvider(): array{
    return [
      [['author'=>'','date'=>'2020-01-01',]], // empty value
      [['author'=>null,'date'=>'2020-01-01',]], // null value
      [['author'=>true,'date'=>'2020-01-01',]], // boolean value
      [['author'=>123,'date'=>'2020-01-01',]], // number value
      [['author'=>'test','date'=>new \DateTime(),]], // object value
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
    $this->expectExceptionMessage('Only string values are allowed, not empty values');

    (new ParquetWriter(new Schema([$id]), $ms))->setCustomMetadata($metadata);
  }

  /**
   * Reading metadata from a file created in pyarrow
   */
  public function testReadMetadata(): void {
    $reader = new ParquetReader($this->openTestFile('movies_pyarrow.parquet'));
    $metadata = $reader->getCustomMetadata();
    $this->assertArrayHasKey( 'great_music', $metadata);
    $this->assertEquals( 'reggaeton', $metadata['great_music']);
  }

}
