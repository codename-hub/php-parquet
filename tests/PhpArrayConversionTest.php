<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetWriter;

use codename\parquet\data\Schema;
use codename\parquet\data\DataType;
use codename\parquet\data\MapField;
use codename\parquet\data\DataField;
use codename\parquet\data\ListField;
use codename\parquet\data\StructField;

use codename\parquet\helper\ArrayToDataColumnsConverter;
use codename\parquet\helper\DataColumnsToArrayConverter;

final class PhpArrayConversionTest extends TestBase
{
  /**
   * [dataProviderTestFiles description]
   * @return array [description]
   */
  public function dataProviderTestFiles(): array {
    return [
      [ 'dates.parquet' ],
      [ 'datetime_other_system.parquet' ],
      [ 'emptycolumn.parquet', [ 'snappy' ] ],
      // [ 'fixedlenbytearray.parquet' ], // Will fail, decimal bug
      // [ 'mixed-dictionary-plain.parquet' ], // Too huge, needs high memory_limit
      // [ 'multi.page.parquet' ], // Too huge, needs high memory_limit
      [ 'postcodes.plain.parquet' ],
      [ 'postcodes.datapageV2.pagesize256.parquet' ],
      [ 'single_nan.parquet', [ 'snappy' ] ],
      [ 'special/multi_data_page.parquet' ],
      [ 'special/multi_page_bit_packed_near_page_border.parquet', [ 'snappy' ] ],
      [ 'special/multi_page_dictionary_with_nulls.parquet' ],
      [ 'real/nation.plain.parquet' ],
    ];
  }

  /**
   * @dataProvider dataProviderTestFiles
   * @param string      $file  [description]
   * @param array|null  $flags
   */
  public function testReadWriteFile(string $file, ?array $flags = null): void {
    // Just to workaround unavailable snappy ext for some files
    if($flags) {
      if(in_array('snappy', $flags)) {
        if(!extension_loaded('snappy')) {
          $this->markTestSkipped('ext-snappy unavailable');
        }
      }
    }

    // We have to increase memory limit for some test files
    // But some or just too huge...
    ini_set('memory_limit', '1G');

    $reader = new ParquetReader($this->openTestFile($file));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    // print_r($reader->schema);
    // print_r($reader2->schema);

    $this->assertEquals($arrayResult, $arrayResult2);
  }

  /**
   * [testDataColumnFieldNotInProvidedSchemaWillFail description]
   */
  public function testDataColumnFieldNotInProvidedSchemaWillFail(): void {
    $schema = new Schema([
      \codename\parquet\data\DataField::createFromType('id1', 'integer')
    ]);
    $columns = [
      // just a blank instance, same DataField name, but different type
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('id1', 'string'), [])
    ];

    $this->expectExceptionMessage('expected a different DataField at position 0');
    $conv = new DataColumnsToArrayConverter($schema, $columns);
  }

  /**
   * [testDifferingFieldCountsSchemaDefinesLessWillFail description]
   */
  public function testDifferingFieldCountsSchemaDefinesLessWillFail(): void {
    $schema = new Schema([
      \codename\parquet\data\DataField::createFromType('id1', 'integer')
    ]);

    $columns = [
      // two fields instead of one
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('id1', 'integer'), []),
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('id2', 'integer'), [])
    ];

    $this->expectExceptionMessage('schema has 1 fields, but only 2 are passed');
    $conv = new DataColumnsToArrayConverter($schema, $columns);
  }

  /**
   * [testDifferingFieldCountsSchemaDefinesMoreWillFail description]
   */
  public function testDifferingFieldCountsSchemaDefinesMoreWillFail(): void {
    $schema = new Schema([
      \codename\parquet\data\DataField::createFromType('id1', 'integer'),
      \codename\parquet\data\DataField::createFromType('id2', 'integer'),
    ]);

    $columns = [
      // one fields instead of one
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('id1', 'integer'), []),
    ];

    $this->expectExceptionMessage('schema has 2 fields, but only 1 are passed');
    $conv = new DataColumnsToArrayConverter($schema, $columns);
  }

  /**
   * Tries to inject a bad map field
   */
  public function testInvalidMapKeyFieldProvided(): void {
    //
    // Bad practice, do not ever try this at home ;-)
    //
    $mapField = new \codename\parquet\data\MapField('someMapField');
    $mapField->key = \codename\parquet\data\StructField::createWithField('structFieldAsMapKeyField', \codename\parquet\data\DataField::createFromType('someIntField', 'integer'));
    $mapField->value = \codename\parquet\data\DataField::createFromType('mapValueField', 'integer');

    $schema = new Schema([
      $mapField
    ]);

    $columns = [
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('someIntField', 'integer'), []),
      new \codename\parquet\data\DataColumn(\codename\parquet\data\DataField::createFromType('mapValueField', 'integer'), []),
    ];

    $this->expectExceptionMessage('expected a different DataField at position 0');
    $conv = new DataColumnsToArrayConverter($schema, $columns);

    // Due to changes in Field::Equals (and overridden methods) this is no longer the expected exception:
    // $this->expectExceptionMessage('Map key field being a non-DataField is not supported');
    // $conv->toArray();
  }

  /**
   * [testDifferingDataFieldPathSchemaNestedMoreDeeplyWillFail description]
   */
  public function testDifferingDataFieldPathSchemaNestedMoreDeeplyWillFail(): void {
    $schema = new Schema([
      // Nested one level
      \codename\parquet\data\StructField::createWithField('someStruct',
        $schemaDataField = \codename\parquet\data\DataField::createFromType('id1', 'integer')
      )
    ]);

    $columnDataField = \codename\parquet\data\DataField::createFromType('id1', 'integer');

    $this->assertNotEquals($schemaDataField->path, $columnDataField->path);

    $columns = [
      // not nested, but same data field name
      // (paths differ)
      new \codename\parquet\data\DataColumn($columnDataField, []),
    ];

    $this->expectExceptionMessage('expected a different DataField at position 0');
    $conv = new DataColumnsToArrayConverter($schema, $columns);

    // Due to changes in Field::Equals (and overridden methods) this is no longer the expected exception:
    // $this->expectExceptionMessage('DataColumn not found for field: id1');
    // $conv->toArray();
  }

  /**
   * [testDifferingDataFieldPathSchemaNestedLessDeeplyWillFail description]
   */
  public function testDifferingDataFieldPathSchemaNestedLessDeeplyWillFail(): void {
    $schema = new Schema([
      // Nested one level
      $schemaDataField = \codename\parquet\data\DataField::createFromType('id1', 'integer')
    ]);

    // Build an arbitrary struct field
    // Doesnt matter after creation, just for propagating levels
    $arbitraryStructField = \codename\parquet\data\StructField::createWithField('someStruct',
      $columnDataField = \codename\parquet\data\DataField::createFromType('id1', 'integer')
    );

    $this->assertNotEquals($schemaDataField->path, $columnDataField->path);

    $columns = [
      // not nested, but same data field name
      // (paths differ)
      new \codename\parquet\data\DataColumn($columnDataField, []),
    ];

    $this->expectExceptionMessage('expected a different DataField at position 0');
    $conv = new DataColumnsToArrayConverter($schema, $columns);

    // Due to changes in Field::Equals (and overridden methods) this is no longer the expected exception:
    // $this->expectExceptionMessage('DataColumn not found for field: id1');
    // $conv->toArray();
  }

  /**
   * A single DataField that is repeated and not nullable
   * and valid data is provided
   */
  public function testArrayFieldNotNullable(): void {
    $schema = new Schema([
      new DataField('arrayfield', DataType::Int32, false, true)
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => [ 6 ] ],
    ];
    $written = $this->writeReadWithData($schema, $data);
    $this->assertEquals($data, $written);
  }

  /**
   * A single DataField that is repeated and not nullable
   * But an empty array is provided
   */
  public function testArrayFieldNotNullableWithEmptyEntryWillFail(): void {
    $schema = new Schema([
      new DataField('arrayfield', DataType::Int32, false, true)
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => [  ] ], // Not allowed
      [ 'arrayfield' => [ 6 ] ],
    ];
    $this->expectExceptionMessageMatches('/Value out of Int32 bounds/');
    $written = $this->writeReadWithData($schema, $data);
  }

  /**
   * A single DataField that is repeated and not nullable
   * But a NULL is provided
   */
  public function testArrayFieldNotNullableWithNullEntryWillFail(): void {
    $schema = new Schema([
      new DataField('arrayfield', DataType::Int32, false, true)
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => null ], // Not allowed
      [ 'arrayfield' => [ 6 ] ],
    ];
    $this->expectExceptionMessageMatches('/Malformed data: null value in not-nullable field/');
    $written = $this->writeReadWithData($schema, $data);
  }

  /**
   * A single DataField that is repeated and nullable
   * And an empty array is provided
   */
  public function testArrayFieldNullable(): void {
    $schema = new Schema([
      new DataField('arrayfield', DataType::Int32, true, true)
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => [  ] ],
      [ 'arrayfield' => [ 6 ] ],
    ];
    $written = $this->writeReadWithData($schema, $data);
    $this->assertEquals($data, $written);
  }

  /**
   * A single DataField that is repeated and nullable
   * An empty array and a NULL is provided
   * To correctly represent this data, an intermediary field
   * would be needed, e.g. a ListField
   * and the field itself would lose its repetitions
   */
  public function testArrayFieldNullableInvalidData(): void {
    $schema = new Schema([
      new DataField('arrayfield', DataType::Int32, true, true)
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => [  ] ],
      [ 'arrayfield' => null ], // This should not be possible and will become []
      [ 'arrayfield' => [ 6 ] ],
    ];
    //
    // WARNING: this case does _NOT_ throw an exception
    // but is silently transformed
    //
    $written = $this->writeReadWithData($schema, $data);
    $this->assertNotEquals($data, $written);
  }

  /**
   * [testArrayFieldNullableWrappedInList description]
   */
  public function testArrayFieldNullableWrappedInList(): void {
    $schema = new Schema([
      new ListField(
        'arrayfield',
        new DataField('f', DataType::Int32, true, false), // Regular field
        true
      )
    ]);
    $data = [
      [ 'arrayfield' => [ 1, 2, 3 ] ],
      [ 'arrayfield' => [ 4, 5 ] ],
      [ 'arrayfield' => [  ] ],     // Empty list
      [ 'arrayfield' => [ null ] ], // Null element
      [ 'arrayfield' => null ],     // Null list
      [ 'arrayfield' => [ 6 ] ],
    ];
    $written = $this->writeReadWithData($schema, $data);
    $this->assertEquals($data, $written);
  }

  /**
   * [writeReadWithData description]
   * @param  Schema $schema
   * @param  array  $data
   * @return array
   */
  protected function writeReadWithData(Schema $schema, array $data): array {
    $dataColumnsConverter = new ArrayToDataColumnsConverter($schema, $data);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    $reader = new ParquetReader($ms);
    $writtenColumns = $reader->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    return $arrayResult2;
  }

  /**
   * [testReadWriteEmptyAndNullStrings description]
   */
  public function testReadWriteEmptyAndNullStrings(): void {
    $schema = new \codename\parquet\data\Schema([
      new \codename\parquet\data\DataField('nullable_string', \codename\parquet\data\DataType::String, true),
      new \codename\parquet\data\DataField('non_nullable_string', \codename\parquet\data\DataType::String, false),
    ]);

    $data = [
      [ 'nullable_string' => 'abc', 'non_nullable_string' => 'abc' ],
      [ 'nullable_string' => '',    'non_nullable_string' => '' ],
      [ 'nullable_string' => null,  'non_nullable_string' => '' ],
      [ 'nullable_string' => 'def', 'non_nullable_string' => 'def' ],
    ];

    $dataColumnsConverter = new ArrayToDataColumnsConverter($schema, $data);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    // Read data again
    $reader = new ParquetReader($ms);
    $readColumns = $reader->ReadEntireRowGroup();

    // read again
    $conv = new DataColumnsToArrayConverter($reader->schema, $readColumns);
    $readData = $conv->toArray();

    $this->assertEquals($data, $readData);
  }

  /**
   * Tests reading a file re-written by spark
   * Which originates from a method below
   * @see testSuperComplexWriteRead
   */
  public function testReadSparkRewrittenSuperComplex(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    // Re-written by spark
    $readerSpark = new ParquetReader($this->openTestFile('custom/supercomplex1.spark.parquet'));
    $rgsSpark = $readerSpark->ReadEntireRowGroup();
    $convSpark = new DataColumnsToArrayConverter($readerSpark->schema, $rgsSpark);
    $dataSpark = $convSpark->toArray();

    // Source, written by us.
    $reader = new ParquetReader($this->openTestFile('custom/supercomplex1.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $dataOriginal = $conv->toArray();

    // Secondary spark result, exported to JSON
    // NOTE: this has to be exported using
    // spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", False)
    // or similar, as Spark will kick out keys of NULL values otherwise
    $jsonData = json_decode(file_get_contents(__DIR__.'/data/custom/supercomplex1.spark.json'), true);

    $this->assertEquals($dataOriginal, $dataSpark);
    $this->assertEquals($dataOriginal, $jsonData);

    foreach($rgs as $idx => $dc) {
      $dcSpark = $rgsSpark[$idx];
      // $this->assertEquals($dc->definitionLevels, $dcSpark->definitionLevels);
      // $this->assertEquals($dc->repetitionLevels, $dcSpark->repetitionLevels);
      $this->assertEquals($dc->getData(), $dcSpark->getData());

      // Spark may modify field names
      // e.g. key and value fields of a map field
      // to the parquet standard defaults
      // so we skip checking name/path
      $this->assertEquals($dc->getField()->dataType, $dcSpark->getField()->dataType);
      // $this->assertEquals($dc->getField()->hasNulls, $dcSpark->getField()->hasNulls);
      $this->assertEquals($dc->getField()->isArray, $dcSpark->getField()->isArray);
      $this->assertEquals($dc->getField()->phpType, $dcSpark->getField()->phpType);
      $this->assertEquals($dc->getField()->schemaType, $dcSpark->getField()->schemaType);
      // $this->assertEquals($dc->getField()->maxDefinitionLevel, $dcSpark->getField()->maxDefinitionLevel);
      // $this->assertEquals($dc->getField()->maxRepetitionLevel, $dcSpark->getField()->maxRepetitionLevel);
    }

    // Write again and read again
    $dataColumnsConverter = new ArrayToDataColumnsConverter($readerSpark->schema, $dataSpark);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($readerSpark->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    $this->assertEquals($dataOriginal, $arrayResult2);
  }

  /**
   * Tests reading a file re-written by spark
   * Which originates from a method below
   * @see testComplexStructsWriteRead
   */
  public function testReadSparkRewrittenComplexStructs(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    // Re-written by spark
    $readerSpark = new ParquetReader($this->openTestFile('custom/complex_structs1.spark.parquet'));
    $rgsSpark = $readerSpark->ReadEntireRowGroup();
    $convSpark = new DataColumnsToArrayConverter($readerSpark->schema, $rgsSpark);
    $dataSpark = $convSpark->toArray();

    // Source, written by us.
    $reader = new ParquetReader($this->openTestFile('custom/complex_structs1.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $dataOriginal = $conv->toArray();

    // Secondary spark result, exported to JSON
    // NOTE: this has to be exported using
    // spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", False)
    // or similar, as Spark will kick out keys of NULL values otherwise
    $jsonData = json_decode(file_get_contents(__DIR__.'/data/custom/complex_structs1.spark.json'), true);

    $this->assertEquals($dataOriginal, $dataSpark);
    $this->assertEquals($dataOriginal, $jsonData);

    foreach($rgs as $idx => $dc) {
      $dcSpark = $rgsSpark[$idx];
      // print_r($dc);
      // print_r($dcSpark);
      // $this->assertEquals($dc->definitionLevels, $dcSpark->definitionLevels);
      // $this->assertEquals($dc->repetitionLevels, $dcSpark->repetitionLevels);
      $this->assertEquals($dc->getData(), $dcSpark->getData());

      // Spark may modify field names
      // e.g. key and value fields of a map field
      // to the parquet standard defaults
      // so we skip checking name/path
      $this->assertEquals($dc->getField()->dataType, $dcSpark->getField()->dataType);
      // $this->assertEquals($dc->getField()->hasNulls, $dcSpark->getField()->hasNulls);
      $this->assertEquals($dc->getField()->isArray, $dcSpark->getField()->isArray);
      $this->assertEquals($dc->getField()->phpType, $dcSpark->getField()->phpType);
      $this->assertEquals($dc->getField()->schemaType, $dcSpark->getField()->schemaType);
      // $this->assertEquals($dc->getField()->maxDefinitionLevel, $dcSpark->getField()->maxDefinitionLevel);
      // $this->assertEquals($dc->getField()->maxRepetitionLevel, $dcSpark->getField()->maxRepetitionLevel);
    }

    // Write again and read again
    $dataColumnsConverter = new ArrayToDataColumnsConverter($readerSpark->schema, $dataSpark);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($readerSpark->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    $this->assertEquals($dataOriginal, $arrayResult2);
  }

  /**
   * Tests reading a special file containing a decimal
   */
  public function testComplexPrimitives(): void {
    $reader = new ParquetReader($this->openTestFile('complex-primitives.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    // NOTE: this is a decimal - luckily, we can directly parse it as a float
    // but originally, this should be a decimal with up to 18 digits after the decimal point
    $this->assertEquals(1.2, $result[0]['decimal']);
  }

  /**
   * Tests reading and writing of some basic datatypes
   */
  public function testWriteAllTypesPlain(): void {
    $reader = new ParquetReader($this->openTestFile('types/alltypes.plain.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();
    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    $this->assertEquals($arrayResult, $arrayResult2);
  }

  /**
   * [dataProviderReadDataColumnIterable description]
   * @return array [description]
   */
  public function dataProviderReadDataColumnIterable(): array {
    return [
      [ 'datetime_other_system.parquet' ],
      [ 'emptycolumn.parquet', [ 'snappy' ] ],
      [ 'customer.impala.parquet' ],
      [ 'special/multi_data_page.parquet' ],
      [ 'postcodes.plain.parquet' ],
      [ 'postcodes.datapageV2.pagesize256.parquet' ],
      [ 'single_nan.parquet', [ 'snappy' ] ],
      [ 'special/multi_data_page.parquet' ],
      [ 'special/multi_page_bit_packed_near_page_border.parquet', [ 'snappy' ] ],
      [ 'special/multi_page_dictionary_with_nulls.parquet' ],
      [ 'real/nation.plain.parquet' ],
      [ 'nested_lists.snappy.parquet', [ 'snappy' ] ],
      [ 'nested_maps.snappy.parquet', [ 'snappy' ] ],
      [ 'nested_structs.parquet', [ 'snappy' ] ],
      [ 'repeated_no_annotation.parquet', [ 'snappy' ] ]
    ];
  }

  /**
   * @dataProvider dataProviderReadDataColumnIterable
   *
   * Tests basic functionality of DataColumnIterable
   * in conjunction with Converters
   *
   * @param string      $file   [description]
   * @param array|null  $flags  [description]
   */
  public function testReadDataColumnIterable(string $file, ?array $flags = null): void {

    // Just to workaround unavailable snappy ext for some files
    if($flags) {
      if(in_array('snappy', $flags)) {
        if(!extension_loaded('snappy')) {
          $this->markTestSkipped('ext-snappy unavailable');
        }
      }
    }

    $options = new \codename\parquet\ParquetOptions();
    // By default, use this one to reduce memory usage for those tests
    $options->TreatByteArrayAsString = true;

    $reader = new ParquetReader($this->openTestFile($file), $options);

    // NOTE: we only load the first row group for these tests
    $rgr = $reader->OpenRowGroupReader(0); // TODO iterate?
    $dataFields = $reader->schema->getDataFields();

    $iterableColumns = [];
    foreach($dataFields as $dataField) {
      $columnReader = $rgr->getDataColumnReader($dataField);
      $iterableColumns[] = $columnReader->getDatacolumnIterable();
    }

    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $iterableColumns);
    $arrayResult = $arrayConverter->toArray();

    // full sequential reading
    $fullReader = new ParquetReader($this->openTestFile($file), $options);
    $columns = $fullReader->ReadEntireRowGroup();
    $fullConv = new DataColumnsToArrayConverter($fullReader->schema, $columns);
    $fullArrayResult = $fullConv->toArray();

    $this->assertCount(count($fullArrayResult), $arrayResult);

    foreach($fullArrayResult as $i => $original) {
      $this->assertEquals($original, $arrayResult[$i], "Inequal dataset at index {$i}");
    }

    // Iterate a second time to make sure the Iterator-methods are implemented correctly
    foreach($fullArrayResult as $i => $original) {
      $this->assertEquals($original, $arrayResult[$i], "Inequal dataset at index {$i}");
    }
  }

  /**
   * Tests reading and writing of nested lists
   */
  public function testWriteNestedLists(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    $reader = new ParquetReader($this->openTestFile('nested_lists.snappy.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    // print_r($arrayResult);
    // print_r($arrayResult2);

    $this->assertEquals($arrayResult, $arrayResult2);
  }


  /**
   * Tests reading and writing of nested structs
   */
  public function testWriteNestedStructs(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    $reader = new ParquetReader($this->openTestFile('nested_structs.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();
    $this->assertEquals($arrayResult, $arrayResult2);

    //
    // make sure the definition levels are the same
    //
    foreach($columns as $idx => $col) {
      $this->assertEquals($col->definitionLevels, $writtenColumns[$idx]->definitionLevels);
      $this->assertEquals($col->repetitionLevels, $writtenColumns[$idx]->repetitionLevels);
    }
  }

  /**
   * Tests reading and writing repeated structs
   * and some specialties
   */
  public function testWriteRepeatedFieldsWithStructs(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    $reader = new ParquetReader($this->openTestFile('repeated_no_annotation.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();

    // make sure its the same as the read datasets
    $this->assertEquals($arrayResult, $arrayResult2);
  }

  /**
   * Tests reading and writing (purely) nested maps
   */
  public function testWriteNestedMaps(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }

    $reader = new ParquetReader($this->openTestFile('nested_maps.snappy.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $arrayConverter = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $arrayResult = $arrayConverter->toArray();

    $dataColumnsConverter = new ArrayToDataColumnsConverter($reader->schema, $arrayResult);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($reader->schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    $reader2 = new ParquetReader($ms);
    $writtenColumns = $reader2->ReadEntireRowGroup();

    // read again
    $arrayConverter2 = new DataColumnsToArrayConverter($reader2->schema, $writtenColumns);
    $arrayResult2 = $arrayConverter2->toArray();
    $this->assertEquals($arrayResult, $arrayResult2);
  }

  /**
   * Tests reading and writing complex structs, partially nullable
   * and highly nested
   */
  public function testComplexStructsWriteRead(): void {
    $schema = new \codename\parquet\data\Schema([
      // Non-nullable struct:
      \codename\parquet\data\StructField::createWithFieldArray(
        'non_nullable_struct1',
        [
          // Non-nullable struct:
          \codename\parquet\data\StructField::createWithFieldArray(
            'non_nullable_struct2',
            [
              // Non-nullable_struct:
              \codename\parquet\data\StructField::createWithFieldArray(
                'non_nullable_struct3',
                [
                  new \codename\parquet\data\DataField('nullable_string', \codename\parquet\data\DataType::String, true),
                  new \codename\parquet\data\DataField('non_nullable_string', \codename\parquet\data\DataType::String, false),
                  new \codename\parquet\data\DataField('nullable_int', \codename\parquet\data\DataType::Int32, true),
                  new \codename\parquet\data\DataField('non_nullable_int', \codename\parquet\data\DataType::Int32, false),
                ]
              )
            ]
          )
        ]
      ),
      // Non-nullable struct:
      \codename\parquet\data\StructField::createWithFieldArray(
        'non_nullable_struct4',
        [
          // Nullable struct:
          \codename\parquet\data\StructField::createWithFieldArray(
            'nullable_struct5',
            [
              // Non-nullable_struct:
              \codename\parquet\data\StructField::createWithFieldArray(
                'non_nullable_struct6',
                [
                  new \codename\parquet\data\DataField('nullable_string2', \codename\parquet\data\DataType::String, true),
                  new \codename\parquet\data\DataField('non_nullable_string2', \codename\parquet\data\DataType::String, false),
                  new \codename\parquet\data\DataField('nullable_int2', \codename\parquet\data\DataType::Int32, true),
                  new \codename\parquet\data\DataField('non_nullable_int2', \codename\parquet\data\DataType::Int32, false),
                ]
              )
            ],
            true // nullable!
          )
        ]
      ),

      // Nullable struct:
      \codename\parquet\data\StructField::createWithFieldArray(
        'nullable_struct7',
        [
          // Non-nullable_struct:
          \codename\parquet\data\StructField::createWithFieldArray(
            'non_nullable_struct8',
            [
              new \codename\parquet\data\DataField('nullable_string3', \codename\parquet\data\DataType::String, true),
              new \codename\parquet\data\DataField('non_nullable_string3', \codename\parquet\data\DataType::String, false),
              new \codename\parquet\data\DataField('nullable_int3', \codename\parquet\data\DataType::Int32, true),
              new \codename\parquet\data\DataField('non_nullable_int3', \codename\parquet\data\DataType::Int32, false),
            ]
          ),

          // Nullable struct:
          \codename\parquet\data\StructField::createWithFieldArray(
            'nullable_struct9',
            [
              new \codename\parquet\data\DataField('nullable_string4', \codename\parquet\data\DataType::String, true),
              new \codename\parquet\data\DataField('non_nullable_string4', \codename\parquet\data\DataType::String, false),
              new \codename\parquet\data\DataField('nullable_int4', \codename\parquet\data\DataType::Int32, true),
              new \codename\parquet\data\DataField('non_nullable_int4', \codename\parquet\data\DataType::Int32, false),
            ],
            true
          )
        ],
        true
      ),

    ]);

    $data = [
      // Dataset index 0
      [
        'non_nullable_struct1' => [
          'non_nullable_struct2' => [
            'non_nullable_struct3' => [
              'nullable_string' => 'ABC',
              'non_nullable_string' => 'ABC',
              'nullable_int' => 123,
              'non_nullable_int' => 123,
            ]
          ]
        ],
        'non_nullable_struct4' => [
          'nullable_struct5' => [
            'non_nullable_struct6' => [
              'nullable_string2' => 'ABC',
              'non_nullable_string2' => 'ABC',
              'nullable_int2' => 123,
              'non_nullable_int2' => 123,
            ]
          ]
        ],
        'nullable_struct7' => null,
      ],

      // Dataset index 1
      [
        'non_nullable_struct1' => [
          'non_nullable_struct2' => [
            'non_nullable_struct3' => [
              'nullable_string' => null,
              'non_nullable_string' => 'DEF',
              'nullable_int' => null,
              'non_nullable_int' => 456,
            ]
          ]
        ],
        'non_nullable_struct4' => [
          'nullable_struct5' => null
        ],
        'nullable_struct7' => [
          'non_nullable_struct8' => [
            'nullable_string3' => 'XYZ',
            'non_nullable_string3' => 'XYZ',
            'nullable_int3' => 987,
            'non_nullable_int3' => 987,
          ],
          'nullable_struct9' => null,
        ],
      ],

      // Dataset index 1
      [
        'non_nullable_struct1' => [
          'non_nullable_struct2' => [
            'non_nullable_struct3' => [
              'nullable_string' => 'GHI',
              'non_nullable_string' => 'GHI',
              'nullable_int' => 789,
              'non_nullable_int' => 789,
            ]
          ]
        ],
        'non_nullable_struct4' => [
          'nullable_struct5' => [
            'non_nullable_struct6' => [
              'nullable_string2' => 'GHI',
              'non_nullable_string2' => 'GHI',
              'nullable_int2' => 789,
              'non_nullable_int2' => 789,
            ]
          ]
        ],
        'nullable_struct7' => [
          'non_nullable_struct8' => [
            'nullable_string3' => null,
            'non_nullable_string3' => 'VWU',
            'nullable_int3' => null,
            'non_nullable_int3' => 654,
          ],
          'nullable_struct9' => [
            'nullable_string4' => null,
            'non_nullable_string4' => 'VWU',
            'nullable_int4' => null,
            'non_nullable_int4' => 654,
          ]
        ],
      ],
    ];

    $dataColumnsConverter = new ArrayToDataColumnsConverter($schema, $data);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();

    fseek($ms, 0);

    // // DEBUG writeout to parquet file
    // $wHandle = fopen(__DIR__.'/data/custom/complex_structs1.parquet', 'w+');
    // stream_copy_to_stream($ms, $wHandle);
    // fclose($wHandle);
    // fseek($ms, 0);

    // Read data again
    $reader = new ParquetReader($ms);
    $readColumns = $reader->ReadEntireRowGroup();

    // read again
    $conv = new DataColumnsToArrayConverter($reader->schema, $readColumns);
    $readData = $conv->toArray();

    // print_r($data);
    // print_r($readData);

    $this->assertEquals($data, $readData);

  }


  /**
   * Some super complex nesting tests - reading and writing
   * A mix of maps, lists, structs and datafields - all nullable
   */
  public function testSuperComplexWriteRead(): void {

    $schema = new \codename\parquet\data\Schema([
      new \codename\parquet\data\MapField(
        'some_map_field',
        \codename\parquet\data\DataField::createFromType('somekey1', 'string'),
        new \codename\parquet\data\ListField(
          'some_list',
          new \codename\parquet\data\MapField(
            'map_field_name',
            \codename\parquet\data\DataField::createFromType('somekey2', 'string'),
            \codename\parquet\data\DataField::createFromType('somevalue', 'string'),
            true, // map itself is nullable
            true  // map's kv can be null (empty)
          ),
          true // list itself is nullable
        ),
        true, // map itself is nullable
        true  // map's kv can be null (empty)
      ),
      new \codename\parquet\data\ListField(
        'list_with_map',
        new \codename\parquet\data\MapField(
          'map_in_list',
          \codename\parquet\data\DataField::createFromType('somekey', 'string'),
          new \codename\parquet\data\ListField(
            'list_in_map_in_list',
            new \codename\parquet\data\MapField(
              'map_in_list_in_map_in_list',
              \codename\parquet\data\DataField::createFromType('innerkey', 'string'),
              \codename\parquet\data\DataField::createFromType('innervalue', 'string'),
              true, // map itself is nullable
              true  // map's kv can be null (empty)
            ),
            true // list itself is nullable
          ),
          true, // map itself is nullable
          true  // map's kv can be null (empty)
        ),
        true // list itself is nullable
      ),
      \codename\parquet\data\StructField::createWithFieldArray(
        'struct_outer',
        [
          new \codename\parquet\data\MapField(
            'map_in_struct',
            \codename\parquet\data\DataField::createFromType('mapkey', 'string'),
            new \codename\parquet\data\ListField(
              'list_in_map_in_struct',
              \codename\parquet\data\DataField::createFromType('list-element', 'string'),
              true
            ),
            true
          ),
          new \codename\parquet\data\ListField(
            'list_in_struct',
            \codename\parquet\data\StructField::createWithFieldArray(
              'struct_in_list_in_struct',
              [
                \codename\parquet\data\DataField::createFromType('inner-struct-data', 'string'),
                new \codename\parquet\data\MapField(
                  'map_in_struct_in_list_in_struct',
                  \codename\parquet\data\DataField::createFromType('inner_key', 'string'),
                  \codename\parquet\data\DataField::createFromType('inner_value', 'string'),
                  true
                )
              ],
              true
            ),
            true
          )
        ],
        true
      )
    ]);


    $data = [

      // Record index 0
      [
        'some_map_field' => [
          // map
          'key0' => [
            // array
            [
              // map
              'a' => 'v1',
              'b' => 'v2',
            ],
            [
              // map
              'c' => 'v3',
              'd' => 'v4',
            ],
          ],
          'key1' => [
            // array
            [
              // map
              'a' => 'v5',
            ],
            [
              // map
              'b' => 'v6',
            ],
            [
              // map
              'c' => 'v7',
              'd' => 'v8',
            ],
          ],
        ],
        'list_with_map' => [
          // array
          // map
          [
            'x' => [
              //list
              [
                // map
                'y' => 'a',
                'z' => 'b',
              ]
            ]
          ],
        ],
        'struct_outer' => [
          'map_in_struct' => [
            'mapkey1' => [ 'string1', 'string2', 'string3' ],
            'mapkey2' => [ 'string4', null, 'string5' ],
            'mapkey3' => [ null ],
            'mapkey4' => [ ],
            'mapkey5' => null,
          ],
          'list_in_struct' => null
        ]
      ],

      // Record index 1
      [
        'some_map_field' => [
          // map
          'key0' => [
            // array
            [
              // map
              'e' => 'v9',
              'f' => 'v10',
            ],
            [
              // map
              'g' => 'v11',
              'h' => 'v12',
            ],
          ],
          'key1' => [
            // empty map/array
          ],
          'key2' => null, // no map/null element
          'key3' => [
            // array
            [
              // map
              'i' => 'v13',
            ],
            [
              // map
              'j' => 'v14',
            ],
            [
              // map
              'k' => 'v15',
              'l' => 'v16',
            ],
          ],
        ],
        'list_with_map' => [
          // array
          // map
          [
            'y' => [
              //list
              [
                // map
                'z' => 'c',
                'a' => 'd',
              ],
              [], // empty map
              null, // null elem
              [], // empty map
            ],
            'z' => [
              // empty list
            ],
          ],
          null, // null element
          [],   // empty map
          [
            // map
            'a' => [
              null,
              [],
              [
                'bbb' => null,
                'ccc' => '',
              ],
              null,
              [
                'g'     => 'h',
                'last'  => 'the very last'
              ]
            ]
          ]
        ],
        'struct_outer' => [
          'map_in_struct'   => null,
          'list_in_struct'  => [] // empty list
        ]
      ],

      // Record index 2
      [
        'some_map_field' => null,
        'list_with_map' => null,
        'struct_outer' => [
          'map_in_struct' => null,
          'list_in_struct' => null,
        ]
      ],

      // Record index 3
      [
        'some_map_field' => null,
        'list_with_map' => null,
        'struct_outer' => null
      ],

      // Record index 4
      [
        'some_map_field' => null,
        'list_with_map' => null,
        'struct_outer' => [
          'map_in_struct'   => [
            // empty map
          ],
          'list_in_struct' => [
            [
              'inner-struct-data' => 'abc',
              'map_in_struct_in_list_in_struct' => [
                // map
                'foo' => 'bar',
                'baz' => 'qux'
              ]
            ],
            // [],
            null,
            [
              'inner-struct-data' => null,
              'map_in_struct_in_list_in_struct' => null
            ],
            [
              'inner-struct-data' => 'def',
              'map_in_struct_in_list_in_struct' => [
                // map
                'bar' => 'baz',
                'qux' => 'quz'
              ]
            ],
          ]
        ]
      ]
    ];

    $dataColumnsConverter = new ArrayToDataColumnsConverter($schema, $data);
    $columns = $dataColumnsConverter->toDataColumns();

    // create a new memory stream to write to
    $ms = fopen('php://memory', 'r+');
    $writer = new ParquetWriter($schema, $ms);
    $rg = $writer->createRowGroup();
    foreach($columns as $c) {
      $rg->WriteColumn($c);
    }
    $rg->finish();
    $writer->finish();


    fseek($ms, 0);

    // // DEBUG writeout to parquet file
    // $wHandle = fopen(__DIR__.'/data/custom/supercomplex1.parquet', 'w+');
    // stream_copy_to_stream($ms, $wHandle);
    // fclose($wHandle);
    // fseek($ms, 0);

    $reader = new ParquetReader($ms);
    $readColumns = $reader->ReadEntireRowGroup();

    // read again
    $conv = new DataColumnsToArrayConverter($reader->schema, $readColumns);
    $readData = $conv->toArray();

    // print_r($data);
    // print_r($readData);

    $this->assertEquals($data, $readData);
  }

  /**
   * Tests various datatypes, not nested
   */
  public function testAllTypesPlain(): void {
    $options = new \codename\parquet\ParquetOptions();
    $options->TreatByteArrayAsString = true;
    $reader = new ParquetReader($this->openTestFile('types/alltypes.plain.parquet'), $options);
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();

    // print_r($result);
    // TODO: assert equality with CSV file
    // $this->assertEquals(1.2, $result[0]['decimal']); // This is from above... remove

    $this->assertEquals([4,5,6,7,2,3,0,1], array_column($result, 'id'));
    $this->assertEquals([true,false,true,false,true,false,true,false], array_column($result, 'bool_col'));
    $this->assertEquals([0,1,0,1,0,1,0,1], array_column($result, 'tinyint_col'));
    $this->assertEquals([0,1,0,1,0,1,0,1], array_column($result, 'smallint_col'));
    $this->assertEquals([0,1,0,1,0,1,0,1], array_column($result, 'int_col'));
    $this->assertEquals([0,10,0,10,0,10,0,10], array_column($result, 'bigint_col'));
    $this->assertEquals([0,1.1,0,1.1,0,1.1,0,1.1], array_column($result, 'float_col'));
    $this->assertEquals([0,10.1,0,10.1,0,10.1,0,10.1], array_column($result, 'double_col'));
    $this->assertEquals(['03/01/09','03/01/09','04/01/09','04/01/09','02/01/09','02/01/09','01/01/09','01/01/09'], array_column($result, 'date_string_col'));
    $this->assertEquals(['0','1','0','1','0','1','0','1'], array_column($result, 'string_col'));

    $this->assertEquals([
      new \DateTimeImmutable('2009-03-01T00:00:00.000000+0000'),
      new \DateTimeImmutable('2009-03-01T00:01:00.000000+0000'),
      new \DateTimeImmutable('2009-04-01T00:00:00.000000+0000'),
      new \DateTimeImmutable('2009-04-01T00:01:00.000000+0000'),
      new \DateTimeImmutable('2009-02-01T00:00:00.000000+0000'),
      new \DateTimeImmutable('2009-02-01T00:01:00.000000+0000'),
      new \DateTimeImmutable('2009-01-01T00:00:00.000000+0000'),
      new \DateTimeImmutable('2009-01-01T00:01:00.000000+0000'),
    ], array_column($result, 'timestamp_col'));
  }

  /**
   * Tests a special case of repeated structs (leading to array data)
   * additionally, this parquet file uses RLE_DICT instead of PLAIN_DICT
   */
  public function testRepeatedFieldsWithStructs(): void {
    $reader = new ParquetReader($this->openTestFile('repeated_no_annotation.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);

    // print_r($reader->getThriftMetadata()->schema);
    // print_r($reader->schema);
    // // die();

    $result = $conv->toArray();

    $this->assertEquals([ 1, 2, 3, 4, 5, 6 ], array_column($result, 'id'));

    $this->assertNull($result[0]['phoneNumbers']);
    $this->assertNull($result[1]['phoneNumbers']);
    $this->assertCount(0, $result[2]['phoneNumbers']['phone']);

    $this->assertCount(1, $result[3]['phoneNumbers']['phone']);
    $this->assertEquals(5555555555, $result[3]['phoneNumbers']['phone'][0]['number']);
    $this->assertEquals(null,       $result[3]['phoneNumbers']['phone'][0]['kind']);

    $this->assertCount(1, $result[4]['phoneNumbers']['phone']);
    $this->assertEquals(1111111111, $result[4]['phoneNumbers']['phone'][0]['number']);
    $this->assertEquals('home',     $result[4]['phoneNumbers']['phone'][0]['kind']);

    $this->assertCount(3, $result[5]['phoneNumbers']['phone']);
    $this->assertEquals(1111111111, $result[5]['phoneNumbers']['phone'][0]['number']);
    $this->assertEquals(2222222222, $result[5]['phoneNumbers']['phone'][1]['number']);
    $this->assertEquals(3333333333, $result[5]['phoneNumbers']['phone'][2]['number']);
    $this->assertEquals('home',     $result[5]['phoneNumbers']['phone'][0]['kind']);
    $this->assertEquals(null,       $result[5]['phoneNumbers']['phone'][1]['kind']);
    $this->assertEquals('mobile',   $result[5]['phoneNumbers']['phone'][2]['kind']);

    // print_r($result); // DEBUG
  }

  /**
   * Tests reading a simple map field
   */
  public function testMapSimple(): void {
    $reader = new ParquetReader($this->openTestFile('map_simple.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    $this->assertEquals([
      [ 'id' => 1, 'numbers' => [ 1 => 'one', 2 => 'two', 3 => 'three' ] ],
    ], $result);
  }

  /**
   * Tests reading/handling of empty lists
   */
  public function testListEmptyAlt(): void {
    $reader = new ParquetReader($this->openTestFile('list_empty_alt.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    $this->assertEquals([
      [ 'id' => 1, 'repeats2' => [ 1, 2, 3 ] ],
      [ 'id' => 2, 'repeats2' => [ ] ],
      [ 'id' => 3, 'repeats2' => [ 1, 2, 3 ] ],
      [ 'id' => 4, 'repeats2' => [ ] ],
    ], $result);
  }

  /**
   * Tests a special case of schema handling
   * Subject's field path is split.leftCategoriesOrThreshold.array
   * So there's no intermediary 'list' field
   * But the element itself is repeated
   */
  public function testLegacyListOneArray(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('legacy-list-onearray.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();

    $this->assertEquals(3,    $result[6]['prediction']);
    $this->assertEquals(0.75, $result[6]['impurity']);
    $this->assertCount(24,    $result[6]['impurityStats']);
    $this->assertEquals(-1,   $result[6]['gain']);

    $this->assertArrayHasKey('featureIndex', $result[6]['split']);
    $this->assertArrayHasKey('leftCategoriesOrThreshold', $result[6]['split']);
    $this->assertArrayHasKey('numCategories', $result[6]['split']);

    // THIS IS WRONG!
    // $this->assertCount(24, $result[6]['split']['leftCategoriesOrThreshold']);

    $this->assertEqualsWithDelta([ 0    ], $result[0]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([ 5.05 ], $result[1]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([ 4.45 ], $result[2]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([      ], $result[3]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([ 4.95 ], $result[4]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([ 4.7  ], $result[5]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);
    $this->assertEqualsWithDelta([      ], $result[6]['split']['leftCategoriesOrThreshold'], static::FP_DELTA);

    // print_r($result); // DEBUG
  }

  const FP_DELTA = 0.00001;

  /**
   * Tests some list columns w/ and w/o nulls
   */
  public function testListColumns(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('list_columns.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    $this->assertEquals([
      [ 'int64_list' => [ 1, 2, 3 ],  'utf8_list' => [ 'abc', 'efg', 'hij' ] ],
      [ 'int64_list' => [ null, 1 ],  'utf8_list' => null ],
      [ 'int64_list' => [ 4 ],        'utf8_list' => [ 'efg', null, 'hij', 'xyz' ] ],
    ], $result);
  }

  /**
   * Tests reading of (purely) nested lists (and their elements)
   */
  public function testNestedLists(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('nested_lists.snappy.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    $this->assertEquals([
      [
        'a' => [[['a', 'b'], ['c']], [null, ['d']]],
        'b' => 1
      ],
      [
        'a' => [[['a', 'b'], ['c', 'd']], [null, ['e']]],
        'b' => 1
      ],
      [
        'a' => [[['a', 'b'], ['c', 'd'], ['e']], [null, ['f']]],
        'b' => 1
      ],
    ], $result);
  }

  /**
   * Tests reading of (purely) nested maps (and their keys/values)
   */
  public function testNestedMaps(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('nested_maps.snappy.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();
    $this->assertEquals([
      [ 'a' => [ 'a' => [ 1 => true, 2 => false ]             ], 'b' => 1, 'c' => 1.0 ],
      [ 'a' => [ 'b' => [ 1 => true             ]             ], 'b' => 1, 'c' => 1.0 ],
      [ 'a' => [ 'c' =>   null                                ], 'b' => 1, 'c' => 1.0 ],
      [ 'a' => [ 'd' => [ ]                                   ], 'b' => 1, 'c' => 1.0 ],
      [ 'a' => [ 'e' => [ 1 => true ]                         ], 'b' => 1, 'c' => 1.0 ],
      [ 'a' => [ 'f' => [ 3 => true, 4 => false, 5 => true ]  ], 'b' => 1, 'c' => 1.0 ],
    ], $result);
  }

  /**
   * Tests reading (purely) nested structs (groups) and their fields
   */
  public function testNestedStructs(): void {
    if(!extension_loaded('snappy')) {
      $this->markTestSkipped('ext-snappy unavailable');
    }
    //
    // NOTE: nested_structs.rust.parquet (the original file)
    // has been compressed using ZSTD (codec 6) and for compatibility,
    // re-written w/o it
    //
    $reader = new ParquetReader($this->openTestFile('nested_structs.parquet'));
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);

    $result = $conv->toArray();

    //
    // nested_structs.parquet has been extracted as json via
    // java -jar .\integration\tools\parquet-tools-1.9.0.jar cat -j .\data\nested_structs.parquet
    // NOTE: a wrapping array ( [ ] ) as been added around the singular dataset returned
    //
    $referenceData = json_decode(file_get_contents(__DIR__.'/data/nested_structs.parquet-mr.json'), true);

    $this->assertEquals($referenceData, $result);
  }

  /**
   * Compare the customer.impala.parquet dataset
   * with its CSV counterpart
   * from repo https://github.com/Parquet/parquet-compatibility
   */
  public function testCustomerImpalaDataset(): void {
    ini_set('memory_limit', '4G'); // needs quite high memory w/o streaming/iterator
    $parquetOptions = new \codename\parquet\ParquetOptions();
    $parquetOptions->TreatByteArrayAsString = true;
    $reader = new ParquetReader($this->openTestFile('customer.impala.parquet'), $parquetOptions);
    $rgs = $reader->ReadEntireRowGroup();
    $conv = new DataColumnsToArrayConverter($reader->schema, $rgs);
    $result = $conv->toArray();

    $handle = fopen(__DIR__.'/data/customer.csv', 'r');

    $keys = [];
    foreach($rgs as $dc) {
      $keys[] = $dc->getField()->name;
    }

    foreach($result as $rowIndex => $row) {
      $csvRow = fgetcsv($handle, 512, '|');
      foreach($csvRow as $colIndex => $value) {
        $compareKey = $keys[$colIndex];
        $this->assertEquals($value, $row[$compareKey]);
      }
    }

    fclose($handle);
  }

}
