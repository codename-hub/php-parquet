<?php
namespace codename\parquet\values\primitives;

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
    /**
     * converts a string decimal to binary data
     */
    /**
     * converts a string decimal to binary data
     */
    /**
     * converts a string decimal to binary data
     */
    public static function DecimalToBinary($d, $precision, $scale) {
        $scaleMultiplier = bcpow(10, $scale, 0);
        $bscaled = gmp_init(bcadd($d, 0, 0));
        $bscaledNum = gmp_strval($bscaled);
        $scaled = bcsub($d, $bscaledNum, $precision);
        $unscaled = bcmul($scaled, $scaleMultiplier, 0);
        $UnscaledValue = gmp_add(gmp_mul($bscaled, $scaleMultiplier), $unscaled);

        $result = array_fill(0, $bufferSize = static::getBufferSize($precision), null);

        $isNegative = gmp_sign($UnscaledValue) === -1;
        if ($isNegative) {
            // Correctly calculate the actual bit length of the absolute value, plus 1 bit for the sign
            $bitLength = strlen(gmp_strval(gmp_abs($UnscaledValue), 2)) + 1;
            $byteLength = (int)ceil($bitLength / 8);
            if ($byteLength === 0) $byteLength = 1;

            // Calculate two's complement for the minimal byte representation length
            $range = gmp_pow(2, $byteLength * 8);
            $twoComplement = gmp_add($range, $UnscaledValue);

            $export = gmp_export($twoComplement, 1, GMP_BIG_ENDIAN | GMP_MSW_FIRST);
            $export = str_pad($export, $byteLength, "\x00", STR_PAD_LEFT);
        } else {
            $export = gmp_export($UnscaledValue, 1, GMP_BIG_ENDIAN | GMP_MSW_FIRST);
            if ($export === '') $export = "\x00";
            if (strlen($export) > 0 && (ord($export[0]) & 0x80)) {
                $export = "\x00" . $export;
            }
        }
        $data = array_reverse(unpack('C*', $export));
        // ----------------------------------------

        if (count($data) > count($result)) throw new Exception("decimal data buffer is ".count($data)." but result must fit into ".count($result)." bytes");

        foreach($data as $i => $v) {
            $result[$i] = $v;
        }

        if (gmp_sign($UnscaledValue) === -1) {
            for ($i = count($data); $i < count($result); $i++) {
                $result[$i] = 0xFF;
            }
        }
        return array_reverse($result);
    }

    /**
     * Converts binary data to a string decimal
     * @param [type]        $data   [description]
     * @param SchemaElement $schema [description]
     */
    public static function BinaryDataToDecimal($data, SchemaElement $schema) {
        $UnscaledValue = gmp_import($data, 1, GMP_BIG_ENDIAN | GMP_MSW_FIRST);

        // Fix PHP 8.1+ deprecation warning by strictly checking the first byte character: $data[0]
        if (strlen($data) > 0 && (ord($data[0]) & 0x80)) {
            $bitLength = strlen($data) * 8;
            $range = gmp_pow(2, $bitLength);
            $UnscaledValue = gmp_sub($UnscaledValue, $range);
        }
        // ------------------------------------------------------

        $precision = $schema->precision;
        $scale = $schema->scale;
        $scaleMultiplier = bcpow(10, $scale, 0);

        list($ipScaled, $fpUnscaled) = \gmp_div_qr($UnscaledValue, $scaleMultiplier);
        $ipScaledStr = gmp_strval($ipScaled);
        $fpScaled = bcdiv(gmp_strval($fpUnscaled), gmp_strval($scaleMultiplier), $precision);
        return bcadd($ipScaledStr, $fpScaled, $precision);
    }

    /**
     * Gets buffer size enough to be able to hold the decimal number of a specific precision
     * @param  int $precision [description]
     * @return int            [length in bytes]
     */
    public static function GetBufferSize(int $precision): int
    {
        switch ($precision) {
            case 1: case 2: return 1;
            case 3: case 4: return 2;
            case 5: case 6: return 3;
            case 7: case 8: case 9: return 4;
            case 10: case 11: return 5;
            case 12: case 13: case 14: return 6;
            case 15: case 16: return 7;
            case 17: case 18: return 8;
            case 19: case 20: case 21: return 9;
            case 22: case 23: return 10;
            case 24: case 25: case 26: return 11;
            case 27: case 28: return 12;
            default: return 16; // Default for higher precision
        }
    }
}
