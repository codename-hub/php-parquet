<?php
namespace codename\parquet\values\primitives;

use Brick\Math\BigDecimal as MathBigDecimal;
use Brick\Math\BigInteger;
use Exception;

use codename\parquet\format\SchemaElement;

class BigDecimal
{
  /**
   * converts a string decimal to binary data
   * @param [type] $d         [description]
   * @param [type] $precision [description]
   * @param [type] $scale     [description]
   */
  public static function DecimalToBinary($d, $precision, $scale) {
    // DecimalValue = d;
    // Precision = precision;
    // Scale = scale;

    // BigInteger scaleMultiplier = BigInteger.Pow(10, scale);
    $scaleMultiplier = bcpow(10, $scale, 0);

    // BigInteger bscaled = new BigInteger(d);
    $bscaled = gmp_init(bcadd($d, 0, 0)); // Trick to strip out the real part, add 0 at 0 precision NOTE: check if we need base 10 for gmp_init?
    $bscaledNum = gmp_strval($bscaled);

    // decimal scaled = d - (decimal)bscaled;
    // only leave fraction?
    $scaled = bcsub($d, $bscaledNum, $precision); // NOTE: At given precision, we need the fraction part
    // $scaled = gmp_sub($d, $bscaled);

    // decimal unscaled = scaled * (decimal)scaleMultiplier;
    $unscaled = bcmul($scaled, $scaleMultiplier, 0);

    // UnscaledValue = (bscaled * scaleMultiplier) + new BigInteger(unscaled);
    $UnscaledValue = gmp_add(gmp_mul($bscaled, $scaleMultiplier), $unscaled);

    // if($debug) {
    //   echo("bscaled");
    //   print_r([
    //     'precision' => $precision,
    //     'scale' => $scale,
    //     '$d' => $d,
    //     '$scaleMultiplier' => $scaleMultiplier,
    //     '$bscaled' => $bscaled,
    //     '$bscaledNum' => $bscaledNum,
    //     '$scaled' => $scaled,
    //     '$unscaled' => $unscaled,
    //     '$UnscaledValue' => $UnscaledValue
    //   ]);
    // }

    //
    // Java: https://docs.oracle.com/javase/7/docs/api/java/math/BigInteger.html#toByteArray()
    //
    // Returns a byte array containing the two's-complement representation of this BigInteger. The byte array will be in big-endian byte-order: the most significant byte is in the zeroth element. The array will contain the minimum number of bytes required to represent this BigInteger, including at least one sign bit, which is (ceil((this.bitLength() + 1)/8)). (This representation is compatible with the (byte[]) constructor.)
    //
    // C#:   https://msdn.microsoft.com/en-us/library/system.numerics.biginteger.tobytearray(v=vs.110).aspx
    //
    //
    //  value | C# | Java
    //
    // -1 | [1111 1111] | [1111 1111] - no difference, so maybe buffer size?
    //

    $result = array_fill(0, $bufferSize = static::getBufferSize($precision), null); // from AllocateResult
    $mbi = BigInteger::of(gmp_strval($UnscaledValue));
    $export = $mbi->toBytes(true);
    $data = unpack('C*', $export);
    $data = array_reverse($data);

    if (count($data) > count($result)) throw new Exception("decimal data buffer is ".count($data)." but result must fit into ".count($result)." bytes");

    // Array.Copy(data, result, data.Length);
    foreach($data as $i => $v) {
      $result[$i] = $v; // chr($v); // TODO/QUESTION: ord or not?
    }

    // // DEBUG
    // $result1 = $result;


    //if value is negative fill the remaining bytes with [1111 1111] i.e. negative flag bit (0xFF)
    if (gmp_sign($UnscaledValue) === -1)
    {
      // if($debug) {
      //   // echo("GMP SIGN -1 ");
      // }
      for ($i = count($data); $i < count($result); $i++)
      {
        $result[$i] = 0xFF;
      }
    }

    $result = array_reverse($result);

    return $result;
  }

  /**
   * Converts binary data to a string decimal
   * @param [type]        $data   [description]
   * @param SchemaElement $schema [description]
   */
  public static function BinaryDataToDecimal($data, SchemaElement $schema) {
    $newUnscaledValue = BigInteger::fromBytes($data);
    $precision = $schema->precision;
    $scale = $schema->scale;
    $scaleMultiplier = bcpow(10, $scale, 0);
    list($newIpScaled, $newFpUnscaled) = $newUnscaledValue->quotientAndRemainder($scaleMultiplier);
    $newFpScaled = MathBigDecimal::of($newFpUnscaled)->dividedBy($scaleMultiplier, $precision);
    $newDecimalValue = MathBigDecimal::of($newIpScaled)->plus($newFpScaled)
      ->toScale($precision);
    return (string) $newDecimalValue;
  }

  /**
   * Gets buffer size enough to be able to hold the decimal number of a specific precision
   * @param  int $precision [description]
   * @return int            [length in bytes]
   */
  public static function GetBufferSize(int $precision): int
  {
    //according to impala source: http://impala.io/doc/html/parquet-common_8h_source.html

    $size = null;

    switch ($precision)
    {
      case 1:
      case 2:
        $size = 1;
        break;
      case 3:
      case 4:
        $size = 2;
        break;
      case 5:
      case 6:
        $size = 3;
        break;
      case 7:
      case 8:
      case 9:
        $size = 4;
        break;
      case 10:
      case 11:
        $size = 5;
        break;
      case 12:
      case 13:
      case 14:
        $size = 6;
        break;
      case 15:
      case 16:
        $size = 7;
        break;
      case 17:
      case 18:
        $size = 8;
        break;
      case 19:
      case 20:
      case 21:
        $size = 9;
        break;
      case 22:
      case 23:
        $size = 10;
        break;
      case 24:
      case 25:
      case 26:
        $size = 11;
        break;
      case 27:
      case 28:
        $size = 12;
        break;
      case 29:
      case 30:
      case 31:
        $size = 13;
        break;
      case 32:
      case 33:
        $size = 14;
        break;
      case 34:
      case 35:
        $size = 15;
        break;
      case 36:
      case 37:
      case 38:
        $size = 16;
        break;
      default:
        $size = 16;
        break;
    }

    return $size;
  }
}
