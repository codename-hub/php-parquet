<?php
namespace jocoon\parquet\values;

use Exception;

use jocoon\parquet\adapter\BinaryWriter;

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
   */
  public static function WriteForwardOnly(BinaryWriter $writer, int $bitWidth, array $data, int $count): void
  {
    //write data to a memory buffer, as we need data length to be written before the data
    // $ms = fopen('php://memory', 'r+');
    $ms = ''; // TEST as string

    // using (var ms = new MemoryStream())
    // {

    // echo("WriteForwardOnly");
    // var_dump($data);

    $bw = \jocoon\parquet\adapter\BinaryWriter::createInstance($ms);
    // $bw->setOrder(\jocoon\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian
    //
    // using (var bw = new BinaryWriter(ms, Encoding.UTF8, true))
    // {
      //write actual data
    static::WriteData($bw, $data, $count, $bitWidth);
    // }

    // int32 - length of data
    // writer.Write((int)ms.Length);
    // $writer->writeInt32($len = fstat($ms)['size']);
    $writer->writeInt32($bw->getEofPosition());

    //actual data
    // ms.Position = 0;
    // fseek($ms, 0);
    $bw->setPosition(0);


    // DEBUG/TODO: do we need to reset this one?
    // $writer->setPosition(0);

    // if($writer instanceof \jocoon\parquet\adapter\BinaryWriter) {
    //   $writer->insert()
    // }
    // $streamContent = stream_get_contents($ms);

    // $writer->insert($bw);

    // stream_copy_to_stream($ms, $writer->getBaseStream());


    $writer->writeString($bw->toString());

    // stream_copy_to_stream($ms, $writer->)
    // ms.CopyTo(writer.BaseStream); //warning! CopyTo performs .Flush internally

    // fclose($ms);
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
    $maxCount = PHP_INT_MAX >> 1;  //max count for an integer with one lost bit
    // TODO/NOTE: change to 32-bit int??

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

    static::WriteUnsignedVarInt($writer, $header);
    static::WriteIntBytes($writer, $value, $byteWidth);
  }

  /**
   * [WriteIntBytes description]
   * @param BinaryWriter $writer    [description]
   * @param int    $value     [description]
   * @param int    $byteWidth [description]
   */
  private static function WriteIntBytes(BinaryWriter $writer, int $value, int $byteWidth): void
  {
    // byte[] dataBytes = BitConverter.GetBytes(value);

    // see https://stackoverflow.com/questions/11544821/how-to-convert-integer-to-byte-array-in-php
    // $dataBytes = pack("L", $value); // unpack("C*", pack("L", $value));
    // $dataBytes = pack('l', $value);
    $dataBytes = unpack("C*", pack("L", $value));

    // echo("_______DATABYTES______".chr(10));
    // echo(" byteWidth=$byteWidth ");
    // var_dump($value);
    // var_dump($dataBytes);
    // // foreach(str_split($dataBytes) as $byte) {
    // //   // code...
    // //   var_dump([$byte, ord($byte)]);
    // // }
    // echo("_______DATABYTES______".chr(10));

    //
    // IMPORTANT NOTE: PHP's unpack/pack functions usually return 1-based index arrays
    // We simply add 1+ in front of each...
    //
    switch($byteWidth)
    {
      case 0:
        break;
      case 1:
        $writer->writeByte($dataBytes[1+0]);
        break;
      case 2:
        $writer->writeByte($dataBytes[1+1]);
        $writer->writeByte($dataBytes[1+0]);
        break;
      case 3:
        $writer->writeByte($dataBytes[1+2]);
        $writer->writeByte($dataBytes[1+1]);
        $writer->writeByte($dataBytes[1+0]);
        break;
      case 4:
        // TODO: Test...
        echo(chr(10)."################################################################################# insertArrayBytes".chr(10));
        $writer->writeBytes($dataBytes);
        break;
      default:
      throw new Exception("encountered bit width ({$byteWidth}) that requires more than 4 bytes.");
    }
  }

  /**
   * [WriteUnsignedVarInt description]
   * @param BinaryWriter $writer [description]
   * @param int    $value  [description]
   */
  private static function WriteUnsignedVarInt(BinaryWriter $writer, int $value): void
  {
    while($value > 127)
    {
      $b = (($value & 0x7F) | 0x80);

      $writer->writeByte($b);

      $value >>= 7;
    }

    $writer->writeByte($value);
  }

}
