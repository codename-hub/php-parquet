php-parquet
===
[![Build Status (Github Actions)](https://github.com/Jocoon/php-parquet/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/Jocoon/php-parquet/actions/workflows/test.yml)

[![GitHub Workflow Status (event)](https://img.shields.io/github/workflow/status/jocoon/php-parquet/Unit%20Tests?event=push&label=release%20build)](https://github.com/Jocoon/php-parquet/actions/workflows/test.yml?query=event%3Apush)
[![GitHub Workflow Status (event)](https://img.shields.io/github/workflow/status/jocoon/php-parquet/Unit%20Tests?event=workflow_dispatch&label=dev%20build)](https://github.com/Jocoon/php-parquet/actions/workflows/test.yml?query=event%3Aworkflow_dispatch)

[![Packagist Version](https://img.shields.io/packagist/v/jocoon/parquet)](https://packagist.org/packages/jocoon/parquet)
[![Packagist PHP Version Support](https://img.shields.io/packagist/php-v/jocoon/parquet)](https://packagist.org/packages/jocoon/parquet)
[![Packagist Downloads](https://img.shields.io/packagist/dt/jocoon/parquet?label=packagist%20installs)](https://packagist.org/packages/jocoon/parquet)

This is the first parquet file format reader/writer implementation in PHP, based on the Thrift sources provided by the Apache Foundation.
Extensive parts of the code and concepts have been ported from parquet-dotnet (see https://github.com/elastacloud/parquet-dotnet and https://github.com/aloneguid/parquet-dotnet).
Therefore, thanks go out to Ivan Gavryliuk (https://github.com/aloneguid).

This package enables you to read and write Parquet files/streams.
It has (almost?) 100% test compatibility with parquet-dotnet, regarding the core functionality, done via PHPUnit.

## Preamble
For some parts of this package, some new patterns had to be invented as I haven't found any implementation that met the requirements.
For most cases, there weren't any implementations available, at all.

Some highlights:

* __GZIP Stream Wrappers__ (that also write headers and checksums) for usage with fopen() and similar functions
* __Snappy Stream Wrappers__ (Snappy compression algorithm) for usage with fopen() and similar functions
* Stream Wrappers that specify/open/wrap a __resource id__ instead of (or in addition to) a file path or URI
* __TStreamTransport__ as a TTransport implementation for pure streaming Thrift data

## Background
I started developing this library due to the fact, there was simply no implementation for PHP.

At my company, we needed a quick solution to archive huge amounts of data from a database in a format that is still queryable, extensible from a schema-perspective and fault-tolerant.
We started testing live 'migrations' via AWS DMS to S3, which ended up crashing on certain amounts of data, due to memory limitations. And it simply was too db-oriented, next to the fact it's easy to accidentally delete data from previous loads.
As we have a heavily SDS-oriented and platform-agnostic architecture, it is not my preferred way to store data as a 1:1 clone of database, like a dump.
Instead, I wanted to have the ability to store data, structured dynamically, like I wanted, in the same way DMS was exporting to S3.
Finally, the project died due to the reasons mentioned above.

But I couldn't get the parquet format out of my head..

The TOP 1 search result (https://stackoverflow.com/questions/44780419/how-to-create-orc-or-parquet-files-from-php-code) looked promising that it would not take that much effort to have a PHP implementation - but in fact, it did take some (about 2 weeks non-consecutive work).
For me, as a PHP and C# developer, parquet-dotnet was a perfect starting point - not merely due to the fact the benchmarks are simply too compelling.
But I expected the PHP implementation not to meet these levels of performance, as this is an initial implementation, showing the principle. And additionally, no one had done it before.

### Raison d'être
As PHP has a huge share regarding web-related projects, this is a _MUST-HAVE_ in times of growing need for big data applications and scenarios.
For my personal motivation, this is a way to show PHP has (physically, virtually?) surpassed it's reputation as a 'scripting language'.
I think - or at least I hope - there are people out there that will benefit from this package and the message it transports. Not only Thrift objects. Pun intended.

## Requirements
You'll need several extensions to use this library to the full extent.

* __bcmath__ (today, this should be a must-have anyway)
* __gmp__ (for working with arbitrary large integers - and indirectly huge decimals!)
* __zlib__ (for GZIP (de-)compression)
* __snappy__ (https://github.com/kjdev/php-ext-snappy - sadly, not published yet to PECL - you'll have to compile it yourself - see Installation)

This library was originally developed to/using PHP 7.3, but it should work on PHP > 7 and will be tested on 8, when released.
At the moment, tests on PHP 7.1 and 7.2 will fail due to some DateTime issues. I'll have a look at it.
Tests fully pass on PHP 7.3 and 7.4. At the time of writing also 8.0.0 RC2 is performing well.

This library highly depends on

* __apache/thrift__ for working with the Thrift-related objects and data
* __nelexa/buffer__ for reading and writing binary data (I decided not to do a C# BinaryWriter clone. (UPDATE 2020-11-04: I just did my own clone, see below.)
* __pear/Math_BigInteger__ for working with binary stored arbitrary-precision decimals (paradox, I know)

As of v0.2, I've also switched to an implementation-agnostic approach of using readers and writers.
Now, we're dealing with BinaryReader(Interface) and BinaryWriter(Interface) implementations that abstract the underlying mechanism.
I've noticed __mdurrant/php-binary-reader__ is just way too slow. I just didn't want to refactor everything just to try out Nelexa's reading powers.
Instead, I've made those two interfaces mentioned above to abstract various packages delivering binary reading/writing.
This finally leads to an optimal way of testing/benchmarking different implementations - and also mixing, e.g. using wapmorgan's package for reading while using Nelexa's for writing.

As of v0.2.1 I've done the binary reader/writer implementations myself, as no implementation met the performance requirements. Especially for writing, this ultra-lightweight implementation delivers thrice* the performance of Nelexa's buffer.
<br><sub><sup><sub><sup>\* intended, I love this word</sup></sub></sup></sub>

Alternative 3rd party binary reading/writing packages in scope:

* __nelexa/buffer__
* __mdurrant/php-binary-reader__ (reading only)
* __wapmorgan/binary-stream__


## Installation
Install this package via composer, e.g.
```bash
composer require jocoon/parquet
```

The included _Dockerfile_ gives you an idea of the needed system requirements.
The most important thing to perform, is to clone and install __php-ext-snappy__.
At the time of writing, it _has not been published do PECL_, yet.

```dockerfile
...
# NOTE: this is a dockerfile snippet. Bare metal machines will be a little bit different

RUN git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git \
  && cd php-ext-snappy \
  && phpize \
  && ./configure \
  && make \
  && make install \
  && docker-php-ext-enable snappy \
  && ls -lna

...
```

Please note: php-ext-snappy is a little bit quirky to compile and install on Windows, so this is just a short information for installation and usage on Linux-based systems.
As long as you don't need the snappy compression for reading or writing, you can use php-parquet without compiling it yourself.

## Helping tools to make life easier
I've found ParquetViewer (https://github.com/mukunku/ParquetViewer) by Mukunku to be a great way of looking into the data to be read or verifying some stuff on a Windows desktop machine.
At least, this helps understanding certain mechanisms, as it more-or-less visually assists by simply displaying the data as a table.

## API
Usage is almost the same as parquet-dotnet. Please note, we have no ```using ( ... ) { }```, like in C#.
So you have to make sure to close/dispose unused resources yourself or let PHP's GC handle it automatically by its refcounting algorithm.
(This is the reason why I don't make use of destructors like parquet-dotnet does.)

### General remarks
As PHP's type system is completely different to C#, we have to make some additions on how to handle certain data types.
For example, a PHP integer is nullable, somehow. An __int__ in C#, isn't. This is a point I'm still unsure about how to deal with it.
For now, I've set int (PHP _integer_) to be nullable - parquet-dotnet is doing this as not-nullable.
You can always adjust this behaviour by manually setting ```->hasNulls = true;``` on your DataField.
Additionally, php-parquet uses a dual way of determining a type.
In PHP, a primitive has it's own type (integer, bool, float/double, etc.).
For class instances (especially DateTime/DateTimeImmutable), the type returned by get_type() is always object.
This is the reason a second property for the DataTypeHandlers exist to match, determine and process it: phpClass.

At the time of writing, not every DataType supported by parquet-dotnet is supported here, too.
F.e. I've skipped Int16, SignedByte and some more, but it shouldn't be too complicated to extend to full binary compatibility.

At the moment, this library serves the __core__ functionality needed for reading and writing parquet files/streams.
It doesn't include parquet-dotnet's Table, Row, Enumerators/helpers from the C# namespace ```Parquet.Data.Rows```.

### Reading files
```php
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

  // $data member, accessible through ->getData() contains an array of column data
  $data = $firstColumn->getData();

  // Print data or do other stuff with it
  print_r($data);
}
```

### Writing files
```php
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
```

## Performance
This package also provides the same benchmark as parquet-dotnet. These are the results on __my machine__:

|                    |Parquet.Net (.NET Core 2.1)|php-parquet (bare metal 7.3)|php-parquet (dockerized* 7.3)|Fastparquet (python)|parquet-mr (Java)|
|--------------------|---------------------------|----------------------------|-----------------------------|--------------------|-----------------|
|Read                |                      255ms|                     1'090ms|                      1'244ms|             154ms**|       _untested_
|Write (uncompressed)|                      209ms|                     1'272ms|                      1'392ms|             237ms**|       _untested_
|Write (gzip)        |                    1'945ms|                     3'314ms|                      3'695ms|           1'737ms**|       _untested_

<small>
* Dockerized on a Windows 10 machine with bind-mounts, which slow down most of those high-IOPS processes.<br>
** It seems fastparquet or Python does some internal caching - the original results on first file opening are way worse (~ 2'700ms)

In general, these tests were performed with gzip compression level 6 for php-parquet. It will roughly halve with 1 (minimum compression) and almost double at 9 (maximum compression).
Note, the latter might not yield the smallest file size, but always the longest compression time.
</small>

## Coding Style
As this is a partial port of a package from a completely different programming language, the programming style is pretty much a pure mess.
I decided to keep most of the casing (e.g. $writer->CreateRowGroup() instead of ->createRowGroup()) to keep a certain 'visual compatibility' to parquet-dotnet.
At least, this is a desirable state from my perspective, as it makes comparing and extending much easier during initial development stages.

## Acknowledgements
Some code parts and concepts have been ported from C#/.NET, see:

* https://github.com/elastacloud/parquet-dotnet
* https://github.com/aloneguid/parquet-dotnet

## License
php-parquet is licensed under the MIT license. See file LICENSE.

## Contributing
You might do a PR, if you want.
Info on how to contribute is coming soon.
