<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\data\Schema;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;

final class PrimitiveTypesTest extends TestBase
{
  /**
   * [testWriteLoadsOfBooleansAllTrue description]
   */
  public function testWriteLoadsOfBooleansAllTrue(): void {
    $counts = [
      100,
      1000
    ];

    foreach($counts as $count) {
      $this->Write_loads_of_booleans_all_true($count);
    }
  }

  /**
   * [Write_loads_of_booleans_all_true description]
   * @param int $count [description]
   */
  public function Write_loads_of_booleans_all_true(int $count):void
  {
    $id = DataField::createFromType("enabled", 'boolean');
    $schema = new Schema([$id]);

    $data = array_fill(0, $count, true);

    $read = $this->WriteReadSingleColumn($id, new DataColumn($id, $data));
    
    for($i = 0; $i < $count; $i++)
    {
      $this->assertTrue($read->getData()[$i], "got FALSE at position {$i}");
    }
  }
}
