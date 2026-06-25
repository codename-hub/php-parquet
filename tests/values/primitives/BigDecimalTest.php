<?php

declare(strict_types=1);

namespace codename\parquet\tests\values\primitives;

use codename\parquet\format\SchemaElement;
use codename\parquet\tests\TestBase;

use codename\parquet\values\primitives\BigDecimal;

final class BigDecimalTest extends TestBase
{
    /**
     * [testValidButMassiveBigDecimal description]
     */
    public function testValidButMassiveBigDecimal(): void
    {
        $bd = BigDecimal::DecimalToBinary("83086059037282.54", 38, 16);
        $this->assertSame([null, null, null, 10, 124, 167, 197, 167, 189, 132, 151, 196, 163, 171, 128, 0], $bd);
    }

    /**
     * Test round-trip conversion for positive, negative, and zero values.
     */
    public function testDecimalToBinaryAndBack(): void
    {
        // Test cases: [string value, precision, scale]
        $testValues = [
            ["83086059037282.54", 38, 16],
            ["-83086059037282.54", 38, 16],
            ["0.00", 38, 16],
            ["1.00", 9, 2],
            ["-1.00", 9, 2]
        ];

        foreach ($testValues as [$originalValue, $precision, $scale]) {
            // Instantiate the real SchemaElement using its array-based constructor
            $schema = new SchemaElement([
                'precision' => $precision,
                'scale' => $scale,
                'name' => 'test_decimal_field'
            ]);

            // 1. Convert string decimal to binary byte array representation
            $binaryArray = BigDecimal::DecimalToBinary($originalValue, $precision, $scale);

            // 2. Pack the byte array back into a binary string for decoding
            // Filter out null placeholders to match binary stream input format
            $filteredBytes = array_filter($binaryArray, function ($byte) {
                return $byte !== null;
            });
            $binaryData = pack('C*', ...$filteredBytes);

            // 3. Decode binary data back to string decimal representation
            $decodedValue = BigDecimal::BinaryDataToDecimal($binaryData, $schema);

            // 4. Assert BCMath-compliant equal values (ignoring trailing zeros after scale conversion)
            $this->assertSame(0, bccomp($originalValue, $decodedValue, $scale), "Failed round-trip for value: {$originalValue}");
        }
    }
}
