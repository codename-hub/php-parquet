<?php

use jocoon\parquet\ParquetOptions;
use jocoon\parquet\ParquetReader;
use jocoon\parquet\ParquetWriter;
use jocoon\parquet\CompressionMethod;

require_once('vendor/autoload.php');

ini_set('memory_limit', '4G');

$readTimes = [];
$uwts = [];
$gwts = [];

for ($i = 0; $i < 10; $i++)
{
  $readTime = null;
  $uwt = null;
  $gwt = null;
  ReadLargeFile($readTime, $uwt, $gwt);
  $readTimes[] = $readTime;
  if($uwt) $uwts[] = $uwt;
  if($gwt) $gwts[] = $gwt;
  echo("iteration #{$i}: {$readTime}, uwt: {$uwt}, gwt: {$gwt}".chr(10));
}

$meanRead = array_sum($readTimes)/count($readTimes);
$meanUw = count($uwts) > 0 ? array_sum($uwts)/count($uwts) : null;
$meanGw = count($gwts) > 0 ? array_sum($gwts)/count($gwts) : null;

echo("mean(read): {$meanRead}, mean(uw): {$meanUw}, mean(gw): {$meanGw}");


function ReadLargeFile(&$readTime, &$uncompressedWriteTime, &$gzipWriteTime)
{
  // Schema schema;
  // DataColumn[] columns;

  $start = hrtime(true);


  $handle = fopen(__DIR__ . '/tests/data/customer.impala.parquet', 'r');

  $opts = new ParquetOptions();
  $opts->TreatByteArrayAsString = true;
  $reader = new ParquetReader($handle, $opts);


  $schema = $reader->schema;
  $cl = [];

  $rgr = $reader->OpenRowGroupReader(0);
  foreach($reader->schema->getDataFields() as $field) {
    $dataColumn = $rgr->ReadColumn($field);
    $cl[] = $dataColumn;
  }
  $columns = $cl;

  $readTime = (hrtime(true) - $start) / 1e9;

  //
  // Writing uncompressed data
  //
  $dest = fopen('perf.uncompressed.parquet', 'w');
  $start = hrtime(true);

  $writer = new ParquetWriter($schema, $dest);
  $writer->compressionMethod = CompressionMethod::None;
  $rg = $writer->CreateRowGroup();
  foreach($columns as $dc) {
    $rg->WriteColumn($dc);
  }
  $rg->finish();
  $writer->finish();

  $uncompressedWriteTime = (hrtime(true) - $start) / 1e9;

  //
  // Writing GZIP compressed data
  //
  $dest = fopen('perf.gzip.parquet', 'w');
  $start = hrtime(true);

  $writer = new ParquetWriter($schema, $dest);
  $writer->compressionMethod = CompressionMethod::Gzip;
  $rg = $writer->CreateRowGroup();
  foreach($columns as $dc) {
    $rg->WriteColumn($dc);
  }
  $rg->finish();
  $writer->finish();

  $gzipWriteTime = (hrtime(true) - $start) / 1e9;

}
