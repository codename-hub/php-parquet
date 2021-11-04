<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetExtensions;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;
use codename\parquet\data\StructField;

final class StructureTest extends TestBase {

  /**
   * [testSimpleStructureWriteRead description]
   */
  public function testSimpleStructureWriteRead(): void
  {
    $schema = new Schema([
      DataField::createFromType('name', 'string'),

      StructField::createWithFieldArray("address", [
        DataField::createFromType('line1', 'string'),
        DataField::createFromType('postcode', 'string'),
      ])
    ]);

    $ms = fopen('php://memory', 'r+');

    // ms.WriteSingleRowGroupParquetFile(schema,
    // new DataColumn(new DataField<string>("name"), new[] { "Ivan" }),
    // new DataColumn(new DataField<string>("line1"), new[] { "woods" }),
    // new DataColumn(new DataField<string>("postcode"), new[] { "postcode" }));
    ParquetExtensions::WriteSingleRowGroupParquetFile($ms, $schema, [
      new DataColumn(DataField::createFromType('name', 'string'), [ "Ivan"] ),
      new DataColumn(DataField::createFromType('line1', 'string'), [ "woods"] ),
      new DataColumn(DataField::createFromType('postcode', 'string'), [ "postcode"] ),
    ]);

    fseek($ms, 0);

    // ms.ReadSingleRowGroupParquetFile(out Schema readSchema, out DataColumn[] readColumns);
    $readSchema = null;
    $readColumns = null;
    ParquetExtensions::ReadSingleRowGroupParquetFile($ms, $readSchema, $readColumns);

    $this->assertEquals("Ivan", $readColumns[0]->getData()[0]);
    $this->assertEquals("woods", $readColumns[1]->getData()[0]);
    $this->assertEquals("postcode", $readColumns[2]->getData()[0]);

    // Assert.Equal("Ivan", readColumns[0].Data.GetValue(0));
    // Assert.Equal("woods", readColumns[1].Data.GetValue(0));
    // Assert.Equal("postcode", readColumns[2].Data.GetValue(0));
  }

}
