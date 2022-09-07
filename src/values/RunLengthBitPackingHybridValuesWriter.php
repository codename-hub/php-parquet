<?php
namespace codename\parquet\values;

use codename\parquet\adapter\BinaryWriter;

/**
 * [RunLengthBitPackingHybridValuesWriter description]
 */
class RunLengthBitPackingHybridValuesWriter
{
  /**
   * [WriteForwardOnly description]
   * @param BinaryWriter $writer   [description]
   * @param int    $bitWidth [description]
   * @param int[]  $data     [description]
   * @param int    $count    [description]
   * @return int Amount of bytes written
   */
  public static function WriteForwardOnly(BinaryWriter $writer, int $bitWidth, array $data, int $count): int
  {
    $startPos = $writer->getPosition();

    //write data to a memory buffer, as we need data length to be written before the data
    // $ms = fopen('php://memory', 'r+');
    $ms = ''; // we simply use a string as target

    $bw = \codename\parquet\adapter\BinaryWriter::createInstance($ms);
    // $bw->setOrder(\codename\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian

    // write actual data
    static::WriteData($bw, $data, $count, $bitWidth);

    // int32 - length of data
    // writer.Write((int)ms.Length);
    // $writer->writeInt32($len = fstat($ms)['size']);
    $writer->writeInt32($bw->getEofPosition());

    // rewind to start for copying to main writer
    // ms.Position = 0;
    // fseek($ms, 0);
    $bw->setPosition(0);

    $writer->writeString($bw->toString());

    return $writer->getPosition() - $startPos; // return amount of bytes written
  }

  /**
   * Pure RLE without size
   * @param  BinaryWriter $writer                 [description]
   * @param  int          $bitWidth               [description]
   * @param  array        $data                   [description]
   * @param  int          $count                  [description]
   * @return int                    [description]
   */
  public static function WritePureRle(BinaryWriter $writer, int $bitWidth, array $data, int $count): int
  {
    $startPos = $writer->getPosition();

    //write data to a memory buffer, as we need data length to be written before the data
    // $ms = fopen('php://memory', 'r+');
    $ms = ''; // we simply use a string as target

    $bw = \codename\parquet\adapter\BinaryWriter::createInstance($ms);
    // $bw->setOrder(\codename\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian

    // write actual data
    static::WriteData($bw, $data, $count, $bitWidth);

    // rewind to start for copying to main writer
    // ms.Position = 0;
    // fseek($ms, 0);
    $bw->setPosition(0);

    $writer->writeString($bw->toString());

    return $writer->getPosition() - $startPos; // return amount of bytes written
  }

  /**
   * [WriteData description]
   * @param BinaryWriter $writer   [description]
   * @param int[]  $data     [description]
   * @param int    $count    [description]
   * @param int    $bitWidth [description]
   */
  private static function WriteData(BinaryWriter $writer, array $data, int $count, int $bitWidth): void
  {
    //for simplicity, we're only going to write RLE, however bitpacking needs to be implemented as well
    $maxCount = 2147483647 >> 1;  //max count for a (signed?) 32-bit integer with one lost bit


    // echo(chr(10)." RLE::WriteData ************************************************* ".chr(10));
    // var_dump([
    //   'data' => $data,
    //   'count' => $count,
    //   'bitWidth' => $bitWidth,
    // ]);
    // echo(chr(10)." RLE::WriteData ************************************************* ".chr(10));

    // echo(" maxCount=$maxCount ");

    //chunk identical values and write
    $lastValue = 0;
    $chunkCount = 0;
    for ($i = 0; $i < $count; $i++)
    {
      $item = $data[$i];

      // print_r([
      //   'action' => 'WriteData FOR',
      //   'vars' => [
      //     '$item' => $item,
      //     '$i' => $i,
      //     '$count' => $count,
      //     '$lastValue' => $lastValue,
      //     '$chunkCount' => $chunkCount,
      //     '$maxCount' => $maxCount,
      //   ]
      // ]);

      if($chunkCount === 0)
      {
        $chunkCount = 1;
        $lastValue = $item;
      }
      else
      {
        // NOTE/TODO: check equality comparator

        // print_r([
        //   'action' => 'WriteData comparison',
        //   'comp' => '$item != $lastValue || $chunkCount == $maxCount',
        //   'res' => "{$item} != {$lastValue} || {$chunkCount} == {$maxCount}",
        //   'vars' => [
        //     '$item' => $item,
        //     '$lastValue' => $lastValue,
        //     '$chunkCount' => $chunkCount,
        //     '$maxCount' => $maxCount,
        //   ]
        // ]);

        if($item != $lastValue || $chunkCount == $maxCount)
        {
          static::WriteRle($writer, $chunkCount, $lastValue, $bitWidth);

          $chunkCount = 1;
          $lastValue = $item;
        }
        else
        {
          $chunkCount += 1;
        }
      }
    }

    if($chunkCount > 0)
    {
      static::WriteRle($writer, $chunkCount, $lastValue, $bitWidth);
    }
  }

  /**
   * [WriteRle description]
   * @param BinaryWriter $writer     [description]
   * @param int    $chunkCount [description]
   * @param int    $value      [description]
   * @param int    $bitWidth   [description]
   */
  private static function WriteRle(BinaryWriter $writer, int $chunkCount, int $value, int $bitWidth): void
  {
    $header = 0x0; // the last bit for RLE is 0
    $header = $chunkCount << 1;
    $byteWidth = (int)(($bitWidth + 7) / 8); //number of whole bytes for this bit width

    // echo(chr(10).'RLEHybridWriter::WriteRle ');
    // var_export([
    //   '$header' => $header,
    //   '$chunkCount' => $chunkCount,
    //   '$byteWidth' => $byteWidth,
    //   'bitWidth' => $bitWidth,
    //   'value' => $value,
    // ]);

    BytesUtils::WriteUnsignedVarInt($writer, $header);
    BytesUtils::WriteIntBytes($writer, $value, $byteWidth);
  }

}
