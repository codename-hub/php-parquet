<?php
namespace jocoon\parquet;

use LogicException;

/**
 * Various (static) stream helpers
 */
class StreamHelper
{
  /**
   *
   */
  public function __construct()
  {
    // Throw an exception, as this is only a static helper class.
    throw new LogicException('Static class');
  }

  /**
   * Marks a stream as 'write finished'
   * This is a helper method, especially for the SnappyInMemoryStreamWrapper
   * (Which GapStreamWrapper also accepts and passes through)
   *
   * @param resource  $resource [the resource to mark as 'finished-writing']
   */
  public static function MarkWriteFinished($resource): void {
    //
    // check if resource is a MarkWriteFinishedInterface-resource
    // luckily, we can easily get the current instance via wrapper_data
    // and directly access some underyling stuff
    //
    $meta = stream_get_meta_data($resource);
    if(($wrapperInstance = $meta['wrapper_data'] ?? null) instanceof MarkWriteFinishedInterface) {
      $wrapperInstance->MarkWriteFinished();
    }
  }
}
