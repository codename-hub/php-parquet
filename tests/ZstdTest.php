<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\StreamHelper;
use codename\parquet\ZstdStreamWrapper;

final class ZstdTest extends TestBase
{
  /**
   * @inheritDoc
   * @requires extension zstd
   */
  protected function setUp(): void
  {
    parent::setUp();
  }

  /**
   * Simple test for a working zstd extension
   */
  public function testSimpleZstd(): void {
    $sampleString = 'sample';
    $compressed = zstd_compress($sampleString);
    $uncompressed = zstd_uncompress($compressed);

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

    ZstdStreamWrapper::register();
    $zstd = ZstdStreamWrapper::createWrappedStream($source, 'r+', ZstdStreamWrapper::MODE_COMPRESS);

    fwrite($zstd, $stage1);
    StreamHelper::MarkWriteFinished($zstd);

    // fseek($source, 0); // ?
    $stage2 = stream_get_contents($source, -1, 0);

    $source = fopen('php://memory', 'r+');
    fwrite($source, $stage2);
    fseek($source, 0);

    $zstd = ZstdStreamWrapper::createWrappedStream($source, 'r+', ZstdStreamWrapper::MODE_DECOMPRESS);

    $ms = fopen('php://memory', 'r+');

    stream_copy_to_stream($zstd, $ms);

    $stage3 = stream_get_contents($ms, -1, 0);

    // validate
    $this->assertEquals($stage1, $stage3);
  }
}
