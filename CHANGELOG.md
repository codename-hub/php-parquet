# Changelog


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
