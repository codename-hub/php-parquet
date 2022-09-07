<?php
namespace codename\parquet\values;

use codename\parquet\adapter\Cast;
use codename\parquet\adapter\BinaryWriter;

/**
 * Methods for working with several byte-based edge cases
 */
class BytesUtils
{
  /**
   * Reads given string an automatically tries to read as Int
   * @param string $data  [bytes]
   */
  public static function ReadIntOnBytes($data)
  {
    switch ($byteWidth = \strlen($data))
    {
      case 0:
        return 0;
      case 1:
        return \ord($data[0]);
      case 2:
        return (\ord($data[1]) << 8) + \ord($data[0]);
      case 3:
        return (\ord($data[2]) << 16) + (\ord($data[1]) << 8) + \ord($data[0]);
      case 4:
        // NOTE: 'l' is 32-bit integer, machine-dependent endianness
        return unpack("l", $data)[1];
        // Alternatively, but may handle sign bit wrong:
        // return (\ord($data[3]) << 24) + (\ord($data[2]) << 16) + (\ord($data[1]) << 8) + \ord($data[0]); // BitConverter.ToInt32(data, 0); // ????
      default:
        throw new \Exception("encountered byte width ($byteWidth) that requires more than 4 bytes.");
    }
  }

  /**
   * Writes a given 32-bit integer value on a reduced byte count.
   * Provides no extra safety if a 4-byte int gets written on a smaller 'target'.
   * @param BinaryWriter $writer
   * @param int          $value
   * @param int          $byteWidth
   */
  public static function WriteIntBytes(BinaryWriter $writer, int $value, int $byteWidth): void
  {
    // see https://github.com/apache/parquet-mr/blob/5608695f5777de1eb0899d9075ec9411cfdf31d3/parquet-common/src/main/java/org/apache/parquet/bytes/BytesUtils.java#L141
    // and https://stackoverflow.com/questions/2642026/php-equivalent-javascript-shift-right-with-zero-fill-bitwise-operators
    // PHP has no unsigned-right-shift operator
    $originalValue = $value;
    $value = Cast::toUnsignedInt($value);
    switch($byteWidth)
    {
      case 0:
        break;
      case 1:
        $writer->writeByte(static::rrr($value, 0) & 0xFF);
        break;
      case 2:
        $writer->writeByte(static::rrr($value, 0) & 0xFF);
        $writer->writeByte(static::rrr($value, 8) & 0xFF);
        break;
      case 3:
        $writer->writeByte(static::rrr($value, 0) & 0xFF);
        $writer->writeByte(static::rrr($value, 8) & 0xFF);
        $writer->writeByte(static::rrr($value, 16) & 0xFF);
        break;
      case 4:
        $writer->writeByte(static::rrr($value, 0) & 0xFF);
        $writer->writeByte(static::rrr($value, 8) & 0xFF);
        $writer->writeByte(static::rrr($value, 16) & 0xFF);
        $writer->writeByte(static::rrr($value, 24) & 0xFF);
        break;
      default:
        throw new \Exception("encountered bit width ({$byteWidth}) that requires more than 4 bytes.");
    }
  }

  /**
   * Unsigned right shift
   * ">>>" Operator as in Java and JavaScript
   * @param  int  $v [byte/int value to be shifted]
   * @param  int  $n [count of bits to be shifted]
   * @return int
   */
  public static function rrr($v, $n) {
    return ($v & 0xFFFFFFFF) >> ($n & 0x1F);
  }

  /**
   * [WriteUnsignedVarInt description]
   * @param BinaryWriter  $writer
   * @param int           $value
   */
  public static function WriteUnsignedVarInt(BinaryWriter $writer, int $value): void
  {
    while($value > 127) {
      $b = (($value & 0x7F) | 0x80);
      $writer->writeByte($b);
      $value >>= 7;
    }
    $writer->writeByte($value);
  }
}
