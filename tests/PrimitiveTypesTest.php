<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\data\Schema;
use codename\parquet\data\DataType;
use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;

final class PrimitiveTypesTest extends TestBase
{
  /**
   * @testWith [100]
   *           [1000]
   * @param int $count [description]
   */
  public function testWriteLoadsOfBooleansAllTrue(int $count):void
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

  /**
   * Maximum value for a 32-bit unsigned integer
   * @var int
   */
  const UIntmaxValue = 4294967295;

  /**
   * @testWith [100]
   * @param int $count  [description]
   */
  public function testWriteBunchOfUInts(int $count): void {

    $id = new DataField('id', DataType::UnsignedInt32, false);

    $data = [];
    for ($i=0; $i < $count; $i++) {
      $data[$i] = static::UIntmaxValue - $i;
    }

    $read = $this->WriteReadSingleColumn($id, new DataColumn($id, $data));
    for ($i=0; $i < $count; $i++) {
      $result = $read->getData()[$i];
      $this->assertEquals(static::UIntmaxValue - $i, $result);
    }
  }
}
