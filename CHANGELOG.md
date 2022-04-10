# Changelog

## [0.6.2 - 2022-04-10]
### Fixed
- Issue #6, wrong bitshift in RLE Reader's ReadIntOnBytes due to syntax misinterpretation

## [0.6.1 - 2021-12-06]
### Fixed
- Missing method `Schema::GetNotEqualsMessage()` and exception/error message on appending to a parquet file with schema mismatch (#3)
### Improved
- Improved return type annotation for `Schema::GetDataFields()` (#1)
- Added PHP 8.1 to Github Actions workflow (Unit Tests)
### Changed
- Removed duplicate Github Actions workflow

## [0.6.0 - 2021-11-10]
### Breaking Change
- Changed namespace to `codename\parquet`
### Added
- Supports for more data types: Int16 (Short), UInt16 (UShort), UInt32, UInt64 (as far as reasonable)
### Improved
- Some micro-optimizations (e.g. in StringDataTypeHandler)
### Fixed
- Port of parquet-dotnet issues/PRs #22 #23 (schema decoding bug, backward compat issue)
### Changed
- Skip some tests if ext-snappy unavailable
- Benchmark: enable snappy compression benchmark if respective extension available/loaded

# Legacy releases (not tagged)

## [0.5.0] - 2021-04-28
### Fixed
- parquet-dotnet PR #96 port - Fix reading of plain dictionary with zero length
- parquet-dotnet PR #95 port - Empty page handling
### Changed
- Replaced dependency "apache/thrift" by "packaged/thrift" to support composer installations under PHP8 without using overrides and implicitly fix some incompatibilities.
- Changed root schema name to `php_parquet_schema` to comply with Avro specs (https://avro.apache.org/docs/current/spec.html#names) and avoid Hadoop failing to read files (see parquet-dotnet issue #93)
- Allow dots in field names
- Provide a greater memory_limit (512M) in phpunit.xml defaults for unit testing

## [0.4.2] - 2020-12-29
### Fixed
- Fixed return type error during Schema Tree traversal when using StructField
### Improved
- EndToEndTypeTests array evaluation/comparison
### Added
- Tests for 'bad data'

## [0.4.1] - 2020-12-10
### Fixed
- Invalid null count determination (parquet-dotnet issue #87) in DataColumnReader

## [0.4.0] - 2020-12-09
### Changed
- ByteArrayDataTypeHandler now really handles byte arrays as arrays of 'bytes' (int arrays in PHP)
### Fixed
- ByteDataTypeHandler improved/fixed according to protocol/specs (single unsigned byte)
- ByteArray datatype-dependent matching
### Added
- PHP 8.0 test pipeline / support
- Test for a pending issue from parquet-dotnet (https://github.com/aloneguid/parquet-dotnet/issues/87) (marked as incomplete as it fails, pending verification)

## [0.3.1] - 2020-11-18
### Changed
- dropped requirement of 'ext-snappy' and made it a suggestion, as it is only required for using snappy compression.
- dropped requirement of 'mdurrant/php-binary-reader', as it has been replaced with a custom implementation

## [0.3.0] - 2020-11-17
### Added
- (Optional) support for calculating statistics (distinct/unique, null, min and max values per column chunk).
  NOTE: this has a negative performance impact, but improves cross-implementation compatibility.
  This has to be enabled via ParquetOptions object: ->CalculateStatistics = true;
  As these fields are mostly optional, they're skipped in favor of performance, by default. Nulls are calculated anyways.
- Quirks Tests for tracking PHP-specific specialties
- Test for parquet-dotnet issue #81 (passing in our package!)
- Test for multi_page_dictionary_with_nulls
- Ported DateTypeHandlerInterface's plainEncode() & plainDecode() for usage with statistics calculations
### Changed
- Automatically return DateTimeDataField when specifying DataField::createFromFormat('xyz', 'DateTimeImmutable') (or DateTimeImmutable::class)
- Minor performance optimizations in ThriftFooter
- enabled RLE-encoding handling in DataColumnReader::readColumn (Tests needed!)
- method signature added/changed: DataTypeHandlerInterface::write(SchemaElement $tse, BinaryWriter $writer, array $values, __DataColumnStatistics__ $statistics)
- Changed some EndToEndTypeTests for accomodate to floating point fluctuations
### Fixed
- Issue with some DateTime conversions having discrepancies of one or more days due to non-float-to-int casting in NanoTime
