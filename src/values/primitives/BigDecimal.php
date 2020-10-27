<?php
namespace jocoon\parquet\values\primitives;

use jocoon\parquet\format\SchemaElement;

class BigDecimal
{
  public static function BinaryDataToDecimal($data, SchemaElement $schema) {
    // $data = array_reverse(chunk_split($data, 1));
    // $data = chunk_split($data, 1);

    // $data = strrev($data);
    // print_r($data);

    // $UnscaledValue = new \Math_BigInteger($data);
    $UnscaledValue = \gmp_import($data); // new BigInteger(data);

    // print_r($UnscaledValue);

    $precision = $schema->precision;
    $scale = $schema->scale;

    // BigInteger scaleMultiplier = BigInteger.Pow(10, Scale);
    // $scaleMultiplier = \gmp_powm(10, $scale);
    $scaleMultiplier = gmp_init(bcpow(10, $scale)); // (new \Math_BigInteger())->powMod();

    // decimal ipScaled = (decimal)BigInteger.DivRem(UnscaledValue, scaleMultiplier, out BigInteger fpUnscaled);
    // list($ipScaled, $fpUnscaled) = $UnscaledValue->divide($UnscaledValue);

    // see https://www.php.net/manual/de/function.gmp-div-qr.php
    list($ipScaled, $fpUnscaled) = \gmp_div_qr($UnscaledValue, $scaleMultiplier);
    // $ipScaled = $res[0];
    // $fpUnscaled = $res[1];

    // $fpScaled = $fpUnscaled / $scaleMultiplier;
    // $fpScaled = \gmp_div_q($fpUnscaled, $scaleMultiplier);

    $ipScaled = gmp_strval($ipScaled);

    $fpScaled = bcdiv(gmp_strval($fpUnscaled), gmp_strval($scaleMultiplier), $precision);

    // DecimalValue = ipScaled + fpScaled;
    $decimalValue = bcadd($ipScaled, $fpScaled, $precision); // $ipScaled + $fpScaled;

    // print_r([
    //   'decimalValue' => $decimalValue,
    //   'ipScaled' => $ipScaled,
    //   'fpScaled' => $fpScaled,
    //   'fpUnscaled' => $fpUnscaled,
    //   'scaleMultiplier' => $scaleMultiplier,
    //   'out' => gmp_strval($decimalValue, 10),
    // ]);

    return $decimalValue;
  }
}
