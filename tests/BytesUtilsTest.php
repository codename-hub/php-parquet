<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\adapter\BinaryReader;
use codename\parquet\adapter\BinaryWriter;

use codename\parquet\values\BytesUtils;

final class BytesUtilsTest extends TestBase
{

  public function readWriteIntOnBytesDataset(): array {
    return [
      [[255, 255, 255, 255],          -1],
      [[255, 255, 255, 254],   -16777217],
      [[255, 128,  64,   1],    21004543],
      [[  1, 128,  64,   1],    21004289],
      [[255, 127,  63,   0],     4161535],
      [[255, 255, 255, 127],  2147483647],
      [[  0,  0,    0,   0],           0],
      [[  1,  0,    0,   0],           1],
      [[  0,  1,    0,   0],         256],
      [[  0,  0,    1,   0],       65536],
      [[  0,  0,    0,   1],    16777216],
      [[128,  0,    0,   0],         128],
      [[  0,  0,    0, 128], -2147483648],
    ];
  }

  /**
   * @dataProvider readWriteIntOnBytesDataset
   */
  public function testReadIntOnBytes($testBytes, $expected) {
    $testByteString = '';
    foreach($testBytes as $b) {
      $testByteString .= chr($b);
    }
    $this->assertEquals($expected, BytesUtils::ReadIntOnBytes($testByteString));
  }

  /**
   * @dataProvider readWriteIntOnBytesDataset
   */
  public function testWriteReadIntOnFourBytes($expectedBytes, $value) {
    $writer = BinaryWriter::createInstance("");
    BytesUtils::WriteIntBytes($writer, $value, 4);
    $byteString = $writer->toString();
    $this->assertEquals($value, BytesUtils::ReadIntOnBytes($byteString));

    $reader = BinaryReader::createInstance($byteString);
    $bytes[] = \ord($reader->readString(1));
    $bytes[] = \ord($reader->readString(1));
    $bytes[] = \ord($reader->readString(1));
    $bytes[] = \ord($reader->readString(1));
    $this->assertEquals($expectedBytes, $bytes);
  }

}
