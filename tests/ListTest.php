<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataField;
use jocoon\parquet\data\ListField;
use jocoon\parquet\data\DataColumn;
use jocoon\parquet\data\StructField;

class ListTest extends TestBase
{

  /**
   * [testListOfStructuresWritesReads description]
   */
  public function testListOfStructuresWritesReads():void
  {
    $idsch = DataField::createFromType('id', 'integer');
    $cnamech = DataField::createFromType('name', 'string');
    $ccountrych = DataField::createFromType('country', 'string');

    $schema = new Schema(
      [
        $idsch,
        new ListField("cities",
          StructField::createWithFieldArray("element", [ // NOTE special static method
            $cnamech,
            $ccountrych
          ])
        )
      ]
    );

    $id = new DataColumn($idsch, [ 1 ]);
    $cname = new DataColumn($cnamech, [ "London", "New York" ], [ 0 ,1 ]);
    $ccountry = new DataColumn($ccountrych, [ "UK", "US" ], [ 0 ,1 ]);

    $readSchema = null;
    $readColumns = $this->WriteReadSingleRowGroup($schema, [ $id, $cname, $ccountry ], $readSchema);
  }

  /**
   * [testListOfElementsWithSomeItemsEmptyReadsFile description]
   */
  public function testListOfElementsWithSomeItemsEmptyReadsFile(): void
  {
    // list data:
    // - 1: [1, 2, 3]
    // - 2: []
    // - 3: [1, 2, 3]
    // - 4: []

    $reader = new ParquetReader($this->openTestFile('list_empty_alt.parquet'));

    $groupReader = $reader->OpenRowGroupReader(0);

    // Assert.Equal(4, groupReader.RowCount);
    // DataField[] fs = reader.Schema.GetDataFields();
    $this->assertEquals(4, $groupReader->getRowCount());
    $fs = $reader->schema->getDataFields();

    // DataColumn id = groupReader.ReadColumn(fs[0]);
    // Assert.Equal(4, id.Data.Length);
    // Assert.False(id.HasRepetitions);

    $id = $groupReader->ReadColumn($fs[0]);
    $this->assertEquals(4, count($id->getData()));
    $this->assertFalse($id->hasRepetitions());

    // DataColumn list = groupReader.ReadColumn(fs[1]);
    // Assert.Equal(8, list.Data.Length);
    // Assert.Equal(new int[] { 0, 1, 1, 0, 0, 1, 1, 0 }, list.RepetitionLevels);
    $list = $groupReader->ReadColumn($fs[1]);
    $this->assertEquals(8, count($list->getData()));
    $this->assertEquals([ 0, 1, 1, 0, 0, 1, 1, 0 ], $list->repetitionLevels);
  }
}
