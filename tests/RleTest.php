<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\adapter\CustomBinaryReader;
use codename\parquet\adapter\CustomBinaryWriter;

use codename\parquet\values\RunLengthBitPackingHybridValuesReader;
use codename\parquet\values\RunLengthBitPackingHybridValuesWriter;

final class RleTest extends TestBase
{
  public function rleDatasets(): array {
    return [
      [[ 0 ]], // TODO
      [[ 0, 0, 0 ]], // TODO

      [[ 1 ]],
      [[ 1, 1, 1 ]],
      [[ 1 ]],
      [[ 1, 1, 1 ]],
      [[ 1, 1, 1, 255, 255 ]],
      [[ 1024, 32768, 32768, 32768 ]],
      [[ 1, 2147483647 ]],
      [[ 1, 1, 2, 3, 2147483647, 2147483647 ]],
      [[ 0, 0, 0, 1, 1, 2, 3, 2147483647, 2147483647 ]],

      // // Negative numbers
      [[ -1, -1, -1, -254, -254, -255, -255, -2147483648, -2147483648 ]]
    ];
  }

  /**
   * Test to make sure bitwidth detection works correctly
   * due to PHP's internal handling as 64-bit signed values
   *
   * @dataProvider rleDatasets
   */
  public function testReadRle(array $data): void {

    $bitWidth = 0;
    foreach($data as $v) {
      $newBitWidth = \codename\parquet\file\DataColumnWriter::getBitWidth($v);
      // echo(" * bit width for $v = $newBitWidth".chr(10));
      $bitWidth = max($bitWidth, $newBitWidth);
    }

    // echo("BitWidth: $bitWidth".chr(10));

    $stream = fopen('php://memory', 'r+');
    $writer = new CustomBinaryWriter($stream);

    $bytesWritten = RunLengthBitPackingHybridValuesWriter::WriteForwardOnly(
      $writer,
      $bitWidth,
      $data,
      count($data)
    );

    // rewind
    fseek($stream, 0);

    $reader = new CustomBinaryReader($stream);

    $dest = [];
    RunLengthBitPackingHybridValuesReader::ReadRleBitpackedHybrid(
      $reader,
      $bitWidth,
      0, // length 0 => use rle header for runlength
      $dest,
      0,
      count($data)
    );
    $this->assertEquals($data, $dest);
  }
}
