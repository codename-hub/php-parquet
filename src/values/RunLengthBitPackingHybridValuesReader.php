<?php
namespace jocoon\parquet\values;

use jocoon\parquet\adapter\BinaryReader;

class RunLengthBitPackingHybridValuesReader
{
  /**
   * [Read description]
   * @param  BinaryReader $reader       [description]
   * @param  int          $bitWidth     [description]
   * @param  array        &$dest         [description]
   * @param  int          $destOffset   [description]
   * @param  int          $maxReadCount [description]
   * @return int                        [description]
   */
  public static function Read(BinaryReader $reader, int $bitWidth, array &$dest, int $destOffset, int $maxReadCount): int {
    $length = static::GetRemainingLength($reader);
    return static::ReadRleBitpackedHybrid($reader, $bitWidth, $length, $dest, $destOffset, $maxReadCount);
  }

  /**
   * [ReadRleBitpackedHybrid description]
   * @param  BinaryReader $reader   [description]
   * @param  int          $bitWidth [description]
   * @param  int          $length   [description]
   * @param  int[]        &$dest     [description]
   * @param  int|null     $offset   [description]
   * @param  int|null     $pageSize [description]
   * @return int                    [description]
   */
  public static function ReadRleBitpackedHybrid(BinaryReader $reader, int $bitWidth, int $length, array &$dest, ?int $offset, ?int $pageSize) : int
  {
     if ($length == 0) {
       $length = $reader->readInt32();
       // echo("ReadRleBitpackedHybrid=$length ".chr(10));
     }

     $start = $reader->getPosition(); //  ftell($reader->getInputHandle()); // $reader->getPosition(); // is this really accessing the base stream?
     $startOffset = $offset;

     // echo("base stream position=".ftell($reader->getInputHandle())." ".chr(10));

     while($reader->getPosition() - $start < $length) {
       $header = static::ReadUnsignedVarInt($reader);
       $isRle = ($header & 1) == 0;

       if ($isRle)
       {
         $offset += static::ReadRle($header, $reader, $bitWidth, $dest, $offset, $pageSize - ($offset - $startOffset));
       }
       else
       {
         $offset += static::ReadBitpacked($header, $reader, $bitWidth, $dest, $offset, $pageSize - ($offset - $startOffset));
       }

       // echo("offset=$offset header=$header".chr(10));
     }

     return $offset - $startOffset;
  }


  /**
   * [ReadRle description]
   * @param  int          $header   [description]
   * @param  BinaryReader $reader   [description]
   * @param  int          $bitWidth [description]
   * @param  int[]        &$dest     [description]
   * @param  int|null     $offset   [description]
   * @param  int          $maxItems [description]
   * @return int                    [description]
   */
  private static function ReadRle(int $header, BinaryReader $reader, int $bitWidth, array &$dest, ?int $offset, int $maxItems) : int
  {
     // The count is determined from the header and the width is used to grab the
     // value that's repeated. Yields the value repeated count times.

     $start = $offset;
     $headerCount = $header >> 1;
     if ($headerCount == 0) return 0; // important not to continue reading as will result in data corruption in data page further
     $count = min($headerCount, $maxItems); // make sure we remain within bounds

     $width = (int)(($bitWidth + 7) / 8); // round up to next byte
     // NOTE: in fact, the line above should not round UP, but instead simply cast to (int) (which causes a round-down in case of 1.5)

     // var_dump([
     //   'bitWidth' => $bitWidth,
     //   '(($bitWidth + 7) / 8)' => (($bitWidth + 7) / 8),
     //   '(int)(($bitWidth + 7) / 8)' => (int)(($bitWidth + 7) / 8),
     //   'width' => $width,
     // ]);

     $data = $reader->readBytes($width);
     $value = static::ReadIntOnBytes($data);

     // echo("ReadIntOnBytes=".ord($value).chr(10));

     for ($i = 0; $i < $count; $i++)
     {
        $dest[$offset++] = $value;
     }

     // echo("dest=".implode($dest).chr(10));

     return $offset - $start;
  }

  /**
   * [ReadBitpacked description]
   * @param  int          $header   [description]
   * @param  BinaryReader $reader   [description]
   * @param  int          $bitWidth [description]
   * @param  int[]        &$dest     [description]
   * @param  int          $offset   [description]
   * @param  int          $maxItems [description]
   * @return int                    [description]
   */
  private static function ReadBitpacked(int $header, BinaryReader $reader, int $bitWidth, array &$dest, int $offset, int $maxItems) : int
  {
     $start = $offset;
     $groupCount = $header >> 1;
     $count = $groupCount * 8;
     $byteCount = (int)(($bitWidth * $count) / 8);
     //int byteCount2 = (int)Math.Ceiling(bitWidth * count / 8.0);

     // var_dump([
     //   '$start' => $start,
     //   '$header' => $header,
     //   '$groupCount' => $groupCount,
     //   '$count' => $count,
     //   '$byteCount' => $byteCount,
     //   '$bitWidth' => $bitWidth,
     // ]);

     //
     // In contrast to the .NET BinaryReader, this one cannot read out-of-boundary
     // Therefore, check remaining bytes.
     //
     $remainingBytes = $reader->getEofPosition() - $reader->getPosition();

     if($remainingBytes < $byteCount) {
       // print_r([
       //   'new_max_byte_count' => true,
       //   '$byteCount' => $byteCount,
       //   '$remainingBytes' => $remainingBytes,
       //   '$reader->getPosition()' => $reader->getPosition(),
       //   '$reader->getEofPosition()' => $reader->getEofPosition(),
       // ]);
       $rawBytes = $reader->readBytes($remainingBytes);
     } else {
       $rawBytes = $reader->readBytes($byteCount);
     }
     $byteCount = \strlen($rawBytes); // TODO: binary safe? // rawBytes.Length;  //sometimes there will be less data available, typically on the last page
     $mask = static::MaskForBits($bitWidth);

     $i = 0;
     $b = \ord($rawBytes[$i]);
     $total = $byteCount * 8;
     $bwl = 8;
     $bwr = 0;
     while ($total >= $bitWidth && ($offset - $start) < $maxItems)
     {
        if ($bwr >= 8)
        {
           $bwr -= 8;
           $bwl -= 8;
           $b >>= 8;
        }
        else if ($bwl - $bwr >= $bitWidth)
        {
           $r = (($b >> $bwr) & $mask);
           $total -= $bitWidth;
           $bwr += $bitWidth;

           $dest[$offset++] = $r; // NOTE: this must not be chr($r) (reversing ord())
        }
        else if ($i + 1 < $byteCount)
        {
           $i += 1;
           $b |= (\ord($rawBytes[$i]) << $bwl);
           $bwl += 8;
        }
     }

     return $offset - $start;
  }

  /**
   * [MaskForBits description]
   * @param  int $width [description]
   * @return int        [description]
   */
  private static function MaskForBits(int $width) : int
  {
     return (1 << $width) - 1;
  }

  private static function ReadIntOnBytes($data)
  {
     switch (\strlen($data))
     {
        case 0:
           return 0;
        case 1:
           return \ord($data[0]);
        case 2:
           return \ord($data[1]) << 8 + \ord($data[0]);
        case 3:
           return \ord($data[2]) << 16 + \ord($data[1]) << 8 + \ord($data[0]);
        case 4:
           return \ord($data[3]) << 32 + \ord($data[2]) << 16 + \ord($data[1]) << 8 + \ord($data[0]); // BitConverter.ToInt32(data, 0); // ????
        default:
           throw new \Exception("encountered byte width (---) that requires more than 4 bytes.");
     }
  }

  /**
   * [ReadUnsignedVarInt description]
   * @param  BinaryReader $reader [description]
   * @return int                  [description]
   */
  private static function ReadUnsignedVarInt(BinaryReader $reader) : int
  {
     $result = 0;
     $shift = 0;

     while (true)
     {
       // echo("BinaryReaderPosition({$reader->getPosition()})");
       //
       // NOTE: we HAVE to use ord() to get the integer value of the byte first
       // PHP does not allow bitshifting in this case, as our byte is returned as a string.
       //
        $b = \ord($reader->readBytes(1)); // was readByte -- readBytes(1) ?

        // echo("byte is: $b ()".chr(10));
        // echo("BinaryReaderPosition({$reader->getPosition()})");

        // echo("ReadUnsignedVarInt:");
        // var_dump($b); // DUNNO. DEBUG/TEST/TODO

        $result |= (($b & 0x7F) << $shift);
        if (($b & 0x80) == 0) break;
        $shift += 7;
     }

     return $result;
  }

  /**
   * [GetRemainingLength description]
   * @param  BinaryReader $reader [description]
   * @return int                  [description]
   */
  private static function GetRemainingLength(BinaryReader $reader): int
  {
    return $reader->getEofPosition() - $reader->getPosition();
  }

}
