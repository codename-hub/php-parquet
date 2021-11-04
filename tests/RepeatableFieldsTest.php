<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\ListField;
use codename\parquet\data\DataColumn;
use codename\parquet\data\StructField;

class RepeatableFieldsTest extends TestBase
{
  /**
   * [testSimpleRepeatedFieldWriteRead description]
   */
  public function testSimpleRepeatedFieldWriteRead():void
  {
    // arrange
    $field = DataField::createFromType('items', 'array', 'integer');

    $column = new DataColumn(
      $field,
      [ 1, 2, 3, 4 ],
      [ 0, 1, 0, 1 ]
    );

    // act
    $rc = $this->WriteReadSingleColumn($field, $column);

    // assert
    $this->assertEquals([ 1, 2, 3, 4 ], $rc->getData());
    $this->assertEquals([ 0, 1, 0, 1 ], $rc->repetitionLevels);

    // NOTE: just leaving this here after porting...
    // https://github.com/elastacloud/parquet-dotnet/blob/final-v2/src/Parquet/File/RepetitionPack.cs
    // tests: https://github.com/elastacloud/parquet-dotnet/blob/final-v2/src/Parquet.Test/RepetitionsTest.cs
  }
}
