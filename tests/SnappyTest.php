<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;

use jocoon\parquet\StreamHelper;
use jocoon\parquet\SnappyInMemoryStreamWrapper;

final class SnappyTest extends TestBase
{
  /**
   * @inheritDoc
   */
  protected function setUp(): void
  {
    if(!extension_loaded('snappy')) {
      static::markTestSkipped('ext-snappy unavailable');
    }
    parent::setUp();
  }

  /**
   * Simple test for a working snappy extension
   */
  public function testSimpleSnappy(): void {
    $sampleString = 'sample';
    $compressed = snappy_compress($sampleString);
    $uncompressed = snappy_uncompress($compressed);

    $this->assertNotFalse($compressed); // Assert it's not FALSE (-> erroneous)
    $this->assertEquals($sampleString, $uncompressed);
  }

  /**
   * [testCompressDecompressRandomByteChunks description]
   */
  public function testCompressDecompressRandomByteChunks(): void {
    for ($i=0; $i < 100; $i++) {
      $this->Compress_decompress_random_byte_chunks($i);
    }
  }

  /**
   * [Compress_decompress_random_byte_chunks description]
   * @param int $index [description]
   */
  public function Compress_decompress_random_byte_chunks(int $index): void
  {
    // $stage1 = RandomGenerator.GetRandomBytes(2, 1000);
    // $stage1 = "A small string to compress esses a a press a mall tring \n"; // (string)random_bytes(1000);
    $stage1 = random_bytes(1000);
    $stage2 = null; // byte[] stage2;
    $stage3 = null; // byte[] stage3;

    $source = fopen('php://memory', 'r+');

    SnappyInMemoryStreamWrapper::register();
    $snappy = SnappyInMemoryStreamWrapper::createWrappedStream($source, 'r+', SnappyInMemoryStreamWrapper::MODE_COMPRESS);

    fwrite($snappy, $stage1);
    StreamHelper::MarkWriteFinished($snappy);

    // fseek($source, 0); // ?
    $stage2 = stream_get_contents($source, -1, 0);

    $source = fopen('php://memory', 'r+');
    fwrite($source, $stage2);
    fseek($source, 0);

    $snappy = SnappyInMemoryStreamWrapper::createWrappedStream($source, 'r+', SnappyInMemoryStreamWrapper::MODE_DECOMPRESS);

    $ms = fopen('php://memory', 'r+');

    stream_copy_to_stream($snappy, $ms);

    $stage3 = stream_get_contents($ms, -1, 0);

    // validate
    $this->assertEquals($stage1, $stage3);
  }
}
