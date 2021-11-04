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
$swts = [];

for ($i = 0; $i < 10; $i++)
{
  $readTime = null;
  $uwt = null;
  $gwt = null;
  $swt = null;
  ReadLargeFile($readTime, $uwt, $gwt, $swt);
  $readTimes[] = $readTime;
  if($uwt) $uwts[] = $uwt;
  if($gwt) $gwts[] = $gwt;
  if($swt) $swts[] = $swt;
  echo("iteration #{$i}: {$readTime}, uwt: {$uwt}, gwt: {$gwt}, swt: {$swt}".chr(10));
}

$meanRead = array_sum($readTimes)/count($readTimes);
$meanUw = count($uwts) > 0 ? array_sum($uwts)/count($uwts) : null;
$meanGw = count($gwts) > 0 ? array_sum($gwts)/count($gwts) : null;
$meanSw = count($swts) > 0 ? array_sum($swts)/count($swts) : null;

echo("mean(read): {$meanRead}, mean(uw): {$meanUw}, mean(gw): {$meanGw}, mean(sw): {$meanSw}");


function ReadLargeFile(&$readTime, &$uncompressedWriteTime, &$gzipWriteTime, &$snappyWriteTime)
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

  // let GC collect
  $reader = null;

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

  // let GC collect
  $writer = null;

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

  //
  // Execute snappy-compression benchmark
  // only of ext is available
  //
  if(extension_loaded('snappy')) {
    //
    // Writing Snappy compressed data
    //
    $dest = fopen('perf.snappy.parquet', 'w');
    $start = hrtime(true);

    $writer = new ParquetWriter($schema, $dest);
    $writer->compressionMethod = CompressionMethod::Snappy;
    $rg = $writer->CreateRowGroup();
    foreach($columns as $dc) {
      $rg->WriteColumn($dc);
    }
    $rg->finish();
    $writer->finish();

    $snappyWriteTime = (hrtime(true) - $start) / 1e9;
  }
}
