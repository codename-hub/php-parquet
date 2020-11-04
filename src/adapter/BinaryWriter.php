<?php
namespace jocoon\parquet\adapter;

/**
 * [abstract description]
 */
abstract class BinaryWriter implements BinaryWriterInterface
{
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
