<?php
namespace jocoon\parquet\adapter;

/**
 * [abstract description]
 */
abstract class BinaryReader implements BinaryReaderInterface
{
  /**
   * [createInstance description]
   * @param  [type] $stream  [description]
   * @param  [type] $options [description]
   * @return BinaryReader
   */
  public static function createInstance($stream, $options = null): BinaryReader {
    return new CustomBinaryReader($stream, $options);
    // return new NelexaBufferBinaryReader($stream, $options); // Preferred implementation at the moment
    // return new WapMorganBinaryReader($stream, $options); // Alternative implementation - needs additional dependencies.
    // return new PhpBinaryReader($stream, $options);       // Legacy implementation. It's simply too slow.
  }
}
