<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\CompressionMethod;
use jocoon\parquet\GzipStreamWrapper;

use jocoon\parquet\data\DataField;

final class CompressionTest extends TestBase
{
  /**
   * Some generic tests to assert libzip/zlib is working the same
   * as our custom GzipStreamWrapper
   */
  public function testGenericGzipCompressionProcedure(): void {

    $sampleString = 'This is a to-be compressed stringgggggg.';
    $compressed = zlib_encode($sampleString, ZLIB_ENCODING_GZIP, 9);

    GzipStreamWrapper::register();

    $ms = fopen('php://memory', 'r+');
    fwrite($ms, $compressed);
    fseek($ms, 0);

    $uncompressedStream = GzipStreamWrapper::createWrappedStream($ms, 'r+', GzipStreamWrapper::MODE_DECOMPRESS);
    $uncompressedContent = stream_get_contents($uncompressedStream);
    $this->assertEquals($sampleString, $uncompressedContent);

    $ms = fopen('php://memory', 'r+');
    $compressedStream = GzipStreamWrapper::createWrappedStream($ms, 'r+', GzipStreamWrapper::MODE_COMPRESS, true);
    fwrite($compressedStream, $sampleString);
    fseek($compressedStream, 0);
    fclose($compressedStream); // Compressed data gets written on close

    $compressedContent = stream_get_contents($ms, -1, 0);
    $uncompressedContent = zlib_decode($compressedContent);

    $this->assertEquals($sampleString, $uncompressedContent);

    $this->assertEquals($compressed, $compressedContent);
  }


  /**
  * [compressionMethods description]
  * @var array
  */
  const compressionMethods = [
    CompressionMethod::None,
    CompressionMethod::Gzip,
    CompressionMethod::Snappy,
  ];

  /**
   * [testAllCompressionMethodsSupportedForSimpleIntegers description]
   */
  public function testAllCompressionMethodsSupportedForSimpleIntegers(): void {
    foreach (static::compressionMethods as $compressionMethod) {
      $this->All_compression_methods_supported_for_simple_strings($compressionMethod);
    }
  }

  /**
   * [All_compression_methods_supported_for_simple_integers description]
   * @param int $compressionMethod [description]
   */
  protected function All_compression_methods_supported_for_simple_integers(int $compressionMethod): void
  {
    $value = 5;
    $actual = $this->WriteReadSingle(DataField::createFromType('id', 'integer'), $value, $compressionMethod);
    // const int value = 5;
    // object actual = WriteReadSingle(new DataField<int>("id"), value, compressionMethod);
    // Assert.Equal(5, (int)actual);

    $this->assertEquals(5, (int)$actual);
  }

  /**
   * [testAllCompressionMethodsSupportedForSimpleStrings description]
   */
  public function testAllCompressionMethodsSupportedForSimpleStrings(): void {
    foreach (static::compressionMethods as $compressionMethod) {
      $this->All_compression_methods_supported_for_simple_strings($compressionMethod);
    }
  }

  /**
   * [All_compression_methods_supported_for_simple_strings description]
   * @param int $compressionMethod [description]
   */
  protected function All_compression_methods_supported_for_simple_strings(int $compressionMethod): void
  {
    /*
    * uncompressed: length - 14, levels - 6
    *
    *
    */

    $value = "five";
    $actual = $this->WriteReadSingle(DataField::createFromType('id', 'string'), $value, $compressionMethod);
    $this->assertEquals("five", $actual);
  }
}
