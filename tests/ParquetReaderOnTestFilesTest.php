<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\ParquetReader;

use jocoon\parquet\helper\OtherExtensions;

final class ParquetReaderOnTestFilesTest extends TestBase
{
  /**
   * [private description]
   * @var array
   */
  private $vals = [
    0x00,
    0x00,
    0x27,
    0x79,
    0x7f,
    0x26,
    0xd6,
    0x71,
    0xc8,
    0x00,
    0x00,
    0x4e,
    0xf2,
    0xfe,
    0x4d,
    0xac,
    0xe3,
    0x8f
  ];

  /**
   * [testFixedLenByteArrayDictionary description]
   */
  public function testFixedLenByteArrayDictionary(): void
  {
    $r = new ParquetReader($this->openTestFile('fixedlenbytearray.parquet'));
    $columns = $r->ReadEntireRowGroup();
  }

  /**
   * [testDatetypesAll description]
   */
  public function testDatetypesAll(): void
  {
    $offset = null;
    $offset2 = null;

    $r = new ParquetReader($this->openTestFile('dates.parquet'));
    $columns = $r->ReadEntireRowGroup();

    $offset = $columns[1]->getData()[0];
    $offset2 = $columns[1]->getData()[1];

    $this->assertEquals(new \DateTime('2017-01-01 00:00:00'), $offset);
    $this->assertEquals(new \DateTime('2017-02-01 00:00:00'), $offset2);
  }

  /**
   * [testDateTimeFromOtherSystem description]
   */
  public function testDateTimeFromOtherSystem(): void
  {
    $r = new ParquetReader($this->openTestFile('datetime_other_system.parquet'));
    $columns = $r->ReadEntireRowGroup();

    $as_at_date_col = null;
    foreach($columns as $x) {
      if($x->getField()->name == 'as_at_date_') {
        $as_at_date_col = $x;
        break;
      }
    }

    $this->assertNotNull($as_at_date_col);

    $offset = OtherExtensions::FromUnixMilliseconds((int)$as_at_date_col->getData()[0]);
    $this->assertEquals(new \DateTime('2018-12-14 00:00:00'), $offset);
  }
}
