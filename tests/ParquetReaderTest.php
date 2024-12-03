<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use Exception;

use codename\parquet\ParquetReader;

use codename\parquet\exception\ArgumentNullException;

final class ParquetReaderTest extends TestBase
{
  /**
   * [testOpeningNullStreamFails description]
   */
  public function testOpeningNullStreamFails(): void
  {
    $this->expectException(ArgumentNullException::class);
    new ParquetReader(null);
  }

  /**
   * [testOpeningSmallFileFails description]
   */
  public function testOpeningSmallFileFails(): void
  {
    $this->expectException(Exception::class);
    $content = 'small';
    $stream = fopen('data://text/plain,' . $content,'r');
    new ParquetReader($stream);
  }

  /**
   * [testOpeningFileWithoutProperHeadFails description]
   */
  public function testOpeningFileWithoutProperHeadFails(): void
  {
    $this->expectException(Exception::class);
    $content = 'PAR2dataPAR1';
    $stream = fopen('data://text/plain,' . $content,'r');
    new ParquetReader($stream);
  }

  /**
   * [testOpeningFileWithoutProperTailFails description]
   */
  public function testOpeningFileWithoutProperTailFails(): void
  {
    $this->expectException(Exception::class);
    $content = 'PAR1dataPAR2';
    $stream = fopen('data://text/plain,' . $content,'r');
    new ParquetReader($stream);
  }

  //
  // NOTE: the following tests are excluded for now
  // as Streams are not as easy to use/fake/mock like in C#.
  //
  // public function testOpening_readable_but_not_seekable_stream_fails(): void
  // {
  //   Assert.Throws<ArgumentException>(() => new ParquetReader(new ReadableNonSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
  // }
  //
  // public function testOpening_not_readable_but_seekable_stream_fails(): void
  // {
  //   Assert.Throws<ArgumentException>(() => new ParquetReader(new NonReadableSeekableStream(new MemoryStream(RandomGenerator.GetRandomBytes(5, 6)))));
  // }


  /**
   * [testReadSimpleMap description]
   */
  public function testReadSimpleMap(): void {
    $reader = new ParquetReader($this->openTestFile('map_simple.parquet'));
    $data = $reader->ReadEntireRowGroup();

    $this->assertEquals([ 1 ], $data[0]->getData());
    $this->assertEquals([ 1, 2, 3 ], $data[1]->getData());
    $this->assertEquals([ 'one', 'two', 'three' ], $data[2]->getData());
  }

  /**
   * [testReadHardcodedDecimal description]
   */
  public function testReadHardcodedDecimal(): void {
    $reader = new ParquetReader($this->openTestFile('complex-primitives.parquet'));
    $value = $reader->ReadEntireRowGroup()[1]->getData()[0];
    $this->assertEquals(1.2, $value); // TODO: check decimal type correctness
  }

  /**
   * [testReadMultiPageFile description]
   */
  public function testReadMultiPageFile(): void {

    //
    // Increase memory limit for this test
    //
    // ini_set('memory_limit', '512M');

    $reader = new ParquetReader($this->openTestFile('multi.page.parquet'));
    $data = $reader->ReadEntireRowGroup();

    $this->assertEquals(927861, count($data[0]->getData()));

    $firstColumn = $data[0]->getData();

    $this->assertEquals(30763, $firstColumn[524286]);
    $this->assertEquals(30766, $firstColumn[524287]);

    //At row 524288 the data is split into another page
    //The column makes use of a dictionary to reduce the number of values and the default dictionary index value is zero (i.e. the first record value)
    $this->assertNotEquals($firstColumn[0], $firstColumn[524288]);

    //The value should be 30768
    $this->assertEquals(30768, $firstColumn[524288]);
  }

  /**
   * [testReadHardcodedDecimal description]
   */
  public function testReadByteArrays(): void {
    $reader = new ParquetReader($this->openTestFile('real/nation.plain.parquet'));

    // NOTE: PHP's unpack() returns byte arrays on a 0-index basis
    $expectedValue = array_values(unpack('C*', 'ALGERIA'));
    $data = $reader->ReadEntireRowGroup();

    $nameColumn = $data[1]->getData();
    $nameValue = $nameColumn[0];

    $this->assertEquals($expectedValue, $nameValue);

    // byte[][] nameColumn = (byte[][]) data[1].Data;
    // nameValue = nameColumn[0];
    // Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue);
    // }
    // Assert.Equal<IEnumerable<byte>>(expectedValue, nameValue); // is this some kind of disposability test?
  }

  /**
   * [testReadMultipleDataPages description]
   */
  public function testReadMultipleDataPages(): void {
    $reader = new ParquetReader($this->openTestFile('special/multi_data_page.parquet'));

    // DataColumn[] columns = reader.ReadEntireRowGroup();
    $columns = $reader->ReadEntireRowGroup();

    // var_dump($columns);

    // string[] s = (string[]) columns[0].Data;
    // double?[] d = (double?[]) columns[1].Data;
    $s = $columns[0]->getData();
    $d = $columns[1]->getData();

    // check for nulls (issue #370)
    for ($i = 0; $i < count($s); $i++)
    {
      // Assert.True(s[i] != null, "found null in s at " + i);
      // Assert.True(d[i] != null, "found null in d at " + i);
      $this->assertTrue($s[$i] !== null, "found null in s at " . $i);
      $this->assertTrue($d[$i] !== null, "found null in d at " . $i);
    }

    // run aggregations checking row alignment (issue #371)
    // var seq = s.Zip(d.Cast<double>(), (w, v) => new {w, v})
    //    .Where(p => p.w == "favorable")
    //    .ToList();

    $seq = [];
    foreach($s as $i => $w) {
      $seq[] = [
        'w' => $w,
        'v' => $d[$i],
      ];
    }
    $seq = array_filter($seq, function($p) {
      return $p['w'] == 'favorable';
    });

    $pv = array_column($seq, 'v');

    // double matching is fuzzy, but matching strings is enough for this test
    // ground truth was computed using Spark

    // As we're might have floats/doubles contained, we need to take care for FP arithmetic quirks
    $this->assertEqualsWithDelta("26706.6185312147", array_sum($pv), 0.00001);
    $this->assertEqualsWithDelta("0.808287234987281", array_sum($pv)/count($pv), 0.00001);
    $this->assertEqualsWithDelta("0.71523915461624", min($pv), 0.00001);
    $this->assertEqualsWithDelta("0.867111980015206", max($pv), 0.00001);

    // Assert.Equal("26706.6185312147", seq.Sum(p => p.v).ToString(CultureInfo.InvariantCulture));
    // Assert.Equal("0.808287234987281", seq.Average(p => p.v).ToString(CultureInfo.InvariantCulture));
    // Assert.Equal("0.71523915461624", seq.Min(p => p.v).ToString(CultureInfo.InvariantCulture));
    // Assert.Equal("0.867111980015206", seq.Max(p => p.v).ToString(CultureInfo.InvariantCulture));
  }

  /**
   * [testReadMultiPageDictionaryWithNulls description]
   */
  public function testReadMultiPageDictionaryWithNulls(): void
  {
    $reader = new ParquetReader($this->openTestFile('special/multi_page_dictionary_with_nulls.parquet'));

    $columns = $reader->ReadEntireRowGroup();

    $rg = $reader->OpenRowGroupReader(0); // not needed?

    // reading columns
    $data = $columns[0]->getData();

    // ground truth from spark
    // check page boundary (first page contains 107432 rows)
    $this->assertEquals("xc3w4eudww", $data[107432]);
    $this->assertEquals("bpywp4wtwk", $data[107433]);
    $this->assertEquals("z6x8652rle", $data[107434]);

    // check near the end of the file
    $this->assertNull($data[310028]);
    $this->assertEquals("wok86kie6c", $data[310029]);
    $this->assertEquals("le9i7kbbib", $data[310030]);
  }

  //
  // NOTE: The following test requires snappy compression
  // Which is not available for Windows at the time of writing.
  //
  /**
   * [testReadBitPackedAtPageBoundary description]
   */
  public function testReadBitPackedAtPageBoundary(): void {
    if(!extension_loaded('snappy')) {
      static::markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('special/multi_page_bit_packed_near_page_border.parquet'));
    $columns = $reader->ReadEntireRowGroup();
    $data = $columns[0]->getData();
    $cnt = count($data);

    // ground truth from spark
    $this->assertEquals(30855, count(array_filter($data, function($v) {
      return $v === null || $v === '';
    })));

    // check page boundary
    $this->assertEquals("collateral_natixis_fr_vol5010", $data[60355]);
    $this->assertEquals("BRAZ82595832_vol16239", $data[60356]);

    // additional, compare pandas result
    $handle = fopen(__DIR__.'/data/special/multi_page_bit_packed_near_page_border.pandas.csv', 'r');
    $row = fgetcsv($handle, null, ',', '"', '\\');
    for ($i=0; $i < $cnt; $i++) {
      $row = fgetcsv($handle, null, ',', '"', '\\');
      $this->assertEquals($data[$i], $row[1]);
    }
    $this->assertFalse(fgetcsv($handle, null, ',', '"', '\\')); // try to read one more
    $this->assertTrue(feof($handle));
    fclose($handle);

    // additional, compare pyarrow result
    $handle = fopen(__DIR__.'/data/special/multi_page_bit_packed_near_page_border.pyarrow.csv', 'r');
    $row = fgetcsv($handle, null, ',', '"', '\\');
    for ($i=0; $i < $cnt; $i++) {
      $row = fgetcsv($handle, null, ',', '"', '\\');
      $this->assertEquals($data[$i], $row[1]);
    }
    $this->assertFalse(fgetcsv($handle, null, ',', '"', '\\')); // try to read one more
    $this->assertTrue(feof($handle));
    fclose($handle);

    // additional, compare fastparquet result
    $handle = fopen(__DIR__.'/data/special/multi_page_bit_packed_near_page_border.fastparquet.csv', 'r');
    $row = fgetcsv($handle, null, ',', '"', '\\');
    for ($i=0; $i < $cnt; $i++) {
      $row = fgetcsv($handle, null, ',', '"', '\\');
      $this->assertEquals($data[$i], $row[1]);
    }
    $this->assertFalse(fgetcsv($handle, null, ',', '"', '\\')); // try to read one more
    $this->assertTrue(feof($handle));
    fclose($handle);
  }

  /**
   * [testReadLargeTimestampData description]
   */
  public function testReadLargeTimestampData(): void
  {
    $reader = new ParquetReader($this->openTestFile('mixed-dictionary-plain.parquet'));
    $columns = $reader->ReadEntireRowGroup();

    // DateTimeOffset?[] col0 = (DateTimeOffset?[])columns[0].Data;
    $col0 = $columns[0]->getData();

    // Assert.Equal(440773, col0.Length);
    $this->assertEquals(440773, count($col0));

    //
    // NOTE: the file does not seem to contain DateTimeOffset data - instead, it's pure ticks.
    //

    $ticks = $col0[0];
    for ($i = 1; $i < 132000; $i++)
    {
      $now = $col0[$i];
      $this->assertNotEquals($ticks, $now);
    }

    // $col00Value = $col0[0];
    //
    // $this->assertTrue($col00Value instanceof \DateTimeInterface);
    //
    // if($col00Value instanceof \DateTimeInterface) {
    //   $ticks = $col00Value->getOffset();
    //
    //   for ($i = 1; $i < 132000; $i++)
    //   {
    //     $colVal = $col0[$i];
    //
    //     $this->assertTrue($colVal instanceof \DateTimeInterface);
    //
    //     if($colVal instanceof \DateTimeInterface) {
    //       $now = $colVal->getOffset();
    //       $this->assertNotEquals($ticks, $now);
    //     }
    //   }
    // } else {
    //   // exception?
    // }
  }

  // public void ParquetReader_OpenFromFile_Close_Stream()
  // {
  //    // copy a file to a temp location
  //    var tempFile = Path.GetTempFileName();
  //    using (var fr = OpenTestFile("map_simple.parquet"))
  //    using (var fw = System.IO.File.OpenWrite(tempFile))
  //    {
  //       fr.CopyTo(fw);
  //    }
  //
  //    // open the copy
  //    using (var reader = ParquetReader.OpenFromFile(tempFile))
  //    {
  //       // do nothing
  //    }
  //
  //    // now try to delete this temp file. If the stream is properly closed, this should succeed
  //    System.IO.File.Delete(tempFile);
  // }

  /**
   * [testReadEmptyColumn description]
   */
  public function testReadEmptyColumn(): void {
    if(!extension_loaded('snappy')) {
      static::markTestSkipped('ext-snappy unavailable');
    }
    $reader = new ParquetReader($this->openTestFile('emptycolumn.parquet'));
    $columns = $reader->ReadEntireRowGroup();
    $col0 = $columns[0]->getData();

    $this->assertCount(10, $col0);
    foreach($col0 as $value) {
      $this->assertNull($value);
    }
  }

}
