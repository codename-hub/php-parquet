<?php
namespace codename\parquet\adapter;

/**
 * [abstract description]
 */
abstract class BinaryWriter implements BinaryWriterInterface
{
  /**
   * [ENDIANESS_BIG_ENDIAN description]
   * @var int
   */
  const BIG_ENDIAN = 1;

  /**
   * [ENDIANNESS_LITTLE_ENDIAN description]
   * @var int
   */
  const LITTLE_ENDIAN = 2;

  /**
   * [createInstance description]
   * @param  [type] $stream  [description]
   * @param  [type] $options [description]
   * @return BinaryWriter
   */
  public static function createInstance($stream, $options = null): BinaryWriter {
    return new CustomBinaryWriter($stream, $options); // Preferred implementation at the moment
    // return new NelexaBufferBinaryWriter($stream, $options); // Preferred implementation at the moment
    // return new WapMorganBinaryWriter($stream, $options); // Alternative implementaion - needs additional dependencies.
  }
}
