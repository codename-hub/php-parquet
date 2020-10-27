<?php
require_once('vendor/autoload.php');
//
// This example script first writes a test file in THIS directory (!).
// Then, we read the first column (the ids)
//

//
// The writing example:
//
use jocoon\parquet\ParquetWriter;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataColumn;

//create data columns with schema metadata and the data you need
$idColumn = new DataColumn(
  DataField::createFromType('id', 'integer'), // NOTE: this is a little bit different to C# due to the type system of PHP
  [ 1, 2 ]
);

$cityColumn = new DataColumn(
  DataField::createFromType('city', 'string'),
  [ "London", "Derby" ]
);

// create file schema
$schema = new Schema([$idColumn->getField(), $cityColumn->getField()]);

// create file handle with w+ flag, to create a new file - if it doesn't exist yet - or truncate, if it exists
$fileStream = fopen(__DIR__.'/test.parquet', 'w+');

$parquetWriter = new ParquetWriter($schema, $fileStream);

// create a new row group in the file
$groupWriter = $parquetWriter->CreateRowGroup();

$groupWriter->WriteColumn($idColumn);
$groupWriter->WriteColumn($cityColumn);

// As we have no 'using' in PHP, I implemented finish() methods
// for ParquetWriter and ParquetRowGroupWriter

$groupWriter->finish();   // finish inner writer(s)
$parquetWriter->finish(); // finish the parquet writer last



//
// The reading example:
//

use jocoon\parquet\ParquetReader;

// open file stream (in this example for reading only)
$fileStream = fopen(__DIR__.'/test.parquet', 'r');

// open parquet file reader
$parquetReader = new ParquetReader($fileStream);

// get file schema (available straight after opening parquet reader)
// however, get only data fields as only they contain data values
$dataFields = $parquetReader->schema->GetDataFields();

// enumerate through row groups in this file
for($i = 0; $i < $parquetReader->getRowGroupCount(); $i++)
{
  // create row group reader
  $groupReader = $parquetReader->OpenRowGroupReader($i);
  // read all columns inside each row group (you have an option to read only
  // required columns if you need to.
  $columns = [];
  foreach($dataFields as $field) {
    $columns[] = $groupReader->ReadColumn($field);
  }

  // get first column, for instance
  $firstColumn = $columns[0];

  // .Data member contains a typed array of column data you can cast to the type of the column
  $data = $firstColumn->getData();

  // Print data or do other stuff with it
  print_r($data);
}
