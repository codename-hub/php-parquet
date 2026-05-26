<?php

/**
 * benchmark-wide-schema.php — reproduces the fast-path speedup measurement
 * referenced in the "Fast-path flat scalar fields" PR against
 * codename/parquet.
 *
 * Usage
 * -----
 *
 * Drop this file at the root of a fresh codename/parquet checkout (or any
 * project where you've `composer require`'d the library), then:
 *
 *     php benchmark-wide-schema.php
 *
 * Run it once on unpatched master, apply the patch, and run it again.
 * The converter timing should drop ~5-10× for the wide tabular case;
 * end-to-end ParquetDataWriter time drops proportionally.
 *
 * The shape exercised here is intentionally typical of CSV/database-style
 * exports: 20,000 rows × 100 columns intermixing integer, bigint, float,
 * string, boolean, and timestamp types, plus one list column to keep the
 * existing recursive (slow path) code exercised. Nullable and
 * non-nullable variants are both present so the patch's null handling
 * gets hit.
 *
 * Output is deterministic so parquet bytes are stable across runs (useful
 * for byte-identity checks when validating the patch).
 */

// Find composer autoloader in either the same dir (typical when dropped at
// project root) or a parent (when dropped in a benchmarks/ subdirectory).
$autoload = null;
foreach ([__DIR__ . '/vendor/autoload.php', __DIR__ . '/../vendor/autoload.php'] as $candidate) {
    if (is_file($candidate)) {
        $autoload = $candidate;
        break;
    }
}
if ($autoload === null) {
    fwrite(STDERR, "Could not find vendor/autoload.php. Run `composer install` first.\n");
    exit(1);
}
require $autoload;

// Wide-table benchmarks balloon PHP row arrays — bump the limit so this
// runs cleanly on a stock php.ini.
ini_set('memory_limit', '1G');

use codename\parquet\data\DataField;
use codename\parquet\data\DateTimeDataField;
use codename\parquet\data\DateTimeFormat;
use codename\parquet\data\ListField;
use codename\parquet\data\Schema;
use codename\parquet\helper\ArrayToDataColumnsConverter;
use codename\parquet\helper\ParquetDataWriter;

const ROW_COUNT = 20_000;
const COL_COUNT = 100;
// Composition of the 100 columns — adjust to test different mixes.
// The list column is intentionally non-trivial: it exercises the existing
// recursive code path so we can verify the patch doesn't change its output.
const COMPOSITION = [
    'integer'   => 10,   // user_id, parent_id, count, etc.
    'bigint'    => 5,    // 64-bit ids
    'float'     => 8,    // score, lat/lng, weights
    'string'    => 12,   // name, email, slug
    'boolean'   => 60,   // pre-computed feature flags (the bulk)
    'timestamp' => 4,    // created_at, updated_at, etc.
    'list'      => 1,    // one tags array column (slow path)
];

// ----------------------------------------------------------------------
// Schema construction
// ----------------------------------------------------------------------

/**
 * @return array{0: array<int, Field>, 1: array<int, array{name: string, type: string, nullable: bool}>}
 */
function buildSchema(): array
{
    $fields = [];
    $colMeta = [];
    $idx = 0;

    foreach (COMPOSITION as $type => $count) {
        for ($i = 0; $i < $count; $i++) {
            $name = sprintf('%s_%02d', $type, $i);
            // Sprinkle nullable fields — every 3rd column for non-bool/non-list types,
            // never for booleans (matches the typical pre-computed feature case).
            $nullable = ($type !== 'boolean' && $type !== 'list' && ($idx % 3 === 0));

            switch ($type) {
                case 'integer':
                    $f = DataField::createFromType($name, 'integer');
                    $f->hasNulls = $nullable;
                    break;

                case 'bigint':
                    // BIGINT also maps to parquet 'integer' in this library
                    // (the underlying handler picks INT64 by default).
                    $f = DataField::createFromType($name, 'integer');
                    $f->hasNulls = $nullable;
                    break;

                case 'float':
                    $f = DataField::createFromType($name, 'double');
                    $f->hasNulls = $nullable;
                    break;

                case 'string':
                    $f = DataField::createFromType($name, 'string');
                    $f->hasNulls = $nullable;
                    break;

                case 'boolean':
                    $f = DataField::createFromType($name, 'boolean');
                    $f->hasNulls = false;
                    break;

                case 'timestamp':
                    $f = DateTimeDataField::create($name, DateTimeFormat::DateAndTime, $nullable);
                    break;

                case 'list':
                    $element = DataField::createFromType('item', 'integer');
                    $element->hasNulls = true;
                    $f = new ListField($name, $element, true);
                    break;

                default:
                    throw new RuntimeException("unknown type {$type}");
            }

            $fields[] = $f;
            $colMeta[] = ['name' => $name, 'type' => $type, 'nullable' => $nullable];
            $idx++;
        }
    }

    if (count($fields) !== COL_COUNT) {
        throw new RuntimeException(
            sprintf('Schema has %d fields, expected %d — adjust COMPOSITION', count($fields), COL_COUNT)
        );
    }

    return [$fields, $colMeta];
}

// ----------------------------------------------------------------------
// Row generation — deterministic so parquet bytes are stable across runs
// ----------------------------------------------------------------------

function buildRows(array $colMeta): array
{
    $rows = [];
    $now = new DateTimeImmutable('2026-01-01 00:00:00', new DateTimeZone('UTC'));

    for ($i = 0; $i < ROW_COUNT; $i++) {
        $row = [];
        foreach ($colMeta as $cIdx => $col) {
            $name = $col['name'];
            $type = $col['type'];
            // ~5% nulls in nullable columns; pseudo-randomly distributed.
            $isNull = $col['nullable'] && ((($i * 7 + $cIdx * 13) % 20) === 0);

            if ($isNull) {
                $row[$name] = null;
                continue;
            }

            switch ($type) {
                case 'integer':
                    $row[$name] = $i + $cIdx;
                    break;
                case 'bigint':
                    $row[$name] = 1_000_000_000 + ($i * 7) + $cIdx;
                    break;
                case 'float':
                    $row[$name] = round(($i * 0.0017) + ($cIdx * 0.13), 4);
                    break;
                case 'string':
                    $row[$name] = sprintf('val_%05d_%02d', $i, $cIdx);
                    break;
                case 'boolean':
                    // ~10% true so the bit-packed output isn't trivially compressible.
                    $row[$name] = (($i * 31 + $cIdx * 17) % 10) === 0;
                    break;
                case 'timestamp':
                    $row[$name] = $now->modify('+' . ($i * 60 + $cIdx) . ' second');
                    break;
                case 'list':
                    // Variable-length list: 0-3 elements. Hits the recursive
                    // (slow-path) code in the converter — same before/after patch.
                    $len = ($i + $cIdx) % 4;
                    $row[$name] = [];
                    for ($k = 0; $k < $len; $k++) {
                        $row[$name][] = $i * 10 + $k;
                    }
                    break;
            }
        }
        $rows[] = $row;
    }
    return $rows;
}

// ----------------------------------------------------------------------
// Bench harness
// ----------------------------------------------------------------------

function fmtTime(float $seconds): string
{
    return $seconds < 1
        ? sprintf('%7.1f ms', $seconds * 1000)
        : sprintf('%7.3f s ', $seconds);
}

function fmtBytes(int $bytes): string
{
    if ($bytes < 1024) {
        return $bytes . ' B';
    }
    if ($bytes < 1024 * 1024) {
        return sprintf('%.1f KiB', $bytes / 1024);
    }
    return sprintf('%.1f MiB', $bytes / 1024 / 1024);
}

[$fields, $colMeta] = buildSchema();
$schema = new Schema($fields);

echo str_repeat('=', 64) . "\n";
echo sprintf("Benchmark: %s rows × %d columns\n", number_format(ROW_COUNT), COL_COUNT);
echo "Composition: ";
foreach (COMPOSITION as $type => $count) {
    echo "{$count} {$type}, ";
}
echo "\n" . str_repeat('=', 64) . "\n\n";

echo "Building rows… ";
$buildStart = microtime(true);
$rows = buildRows($colMeta);
echo fmtTime(microtime(true) - $buildStart) . "\n\n";

// ---- 1. Converter in isolation (the function the patch optimizes) ----
$peakBefore = memory_get_peak_usage(true);
$t = microtime(true);
$converter = new ArrayToDataColumnsConverter($schema, $rows);
$dataColumns = $converter->toDataColumns();
$convT = microtime(true) - $t;
$convPeak = memory_get_peak_usage(true) - $peakBefore;

echo "ArrayToDataColumnsConverter::toDataColumns\n";
echo "  time:        " . fmtTime($convT) . "\n";
echo "  peak Δ mem:  " . fmtBytes(max(0, $convPeak)) . "\n";
echo "  data cols:   " . count($dataColumns) . " (sanity check vs " . COL_COUNT . " schema fields)\n\n";

// ---- 2. End-to-end parquet write (in-memory) ----
$handle = fopen('php://memory', 'w+');
$t = microtime(true);
$writer = new ParquetDataWriter($handle, $schema);
$writer->putBatch($rows);
$writer->finish();
$writeT = microtime(true) - $t;
$bytes = ftell($handle);
fclose($handle);

echo "End-to-end ParquetDataWriter → php://memory\n";
echo "  time:        " . fmtTime($writeT) . "\n";
echo "  file size:   " . fmtBytes($bytes) . "\n\n";

echo str_repeat('-', 64) . "\n";
echo "Apply the fast-path patch and rerun to compare. The converter\n";
echo "line is the most direct measurement of the patch's effect;\n";
echo "end-to-end write time is the practical user-facing impact.\n";
