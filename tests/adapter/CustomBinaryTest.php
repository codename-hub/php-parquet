<?php
declare(strict_types=1);
namespace codename\parquet\tests\adapter;

use codename\parquet\adapter\CustomBinaryReader;
use codename\parquet\adapter\CustomBinaryWriter;

class CustomBinaryTest extends AbstractBinaryReadingWritingTest {
  /**
   * @inheritDoc
   */
  protected function getBinaryReader(
    $stream
  ): \codename\parquet\adapter\BinaryReaderInterface {
    return new CustomBinaryReader($stream);
  }

  /**
   * @inheritDoc
   */
  protected function getBinaryWriter(
    $stream
  ): \codename\parquet\adapter\BinaryWriterInterface {
    return new CustomBinaryWriter($stream);
  }
}
