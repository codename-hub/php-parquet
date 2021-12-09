<?php
declare(strict_types=1);
namespace codename\parquet\tests\adapter;

use codename\parquet\adapter\NelexaBufferBinaryReader;
use codename\parquet\adapter\NelexaBufferBinaryWriter;


class NelexaBinaryTest extends AbstractBinaryReadingWritingTest {
  /**
   * @inheritDoc
   */
  protected function getBinaryReader(
    $stream
  ): \codename\parquet\adapter\BinaryReaderInterface {
    return new NelexaBufferBinaryReader($stream);
  }

  /**
   * @inheritDoc
   */
  protected function getBinaryWriter(
    $stream
  ): \codename\parquet\adapter\BinaryWriterInterface {
    return new NelexaBufferBinaryWriter($stream);
  }
}
