<?php
namespace jocoon\parquet;

use Exception;


/**
 * [SnappyInMemoryStreamWrapper description]
 */
class SnappyInMemoryStreamWrapper implements StreamWrapperInterface, MarkWriteFinishedInterface
{
  /**
   * [createWrappedStream description]
   * @param resource  $stream           [description]
   * @param string    $mode             [read-write-mode]
   * @param int       $compressionMode  [compression mode]
   * @param bool      $leaveOpen        [whether to leave underlying stream open on closing of this stream - by default, this is true for this type of stream]
   * @return resource
   */
  public static function createWrappedStream($stream, $mode, int $compressionMode, bool $leaveOpen = true) {
    $context = stream_context_create([
      'snappy' => [
        'leave_open'        => $leaveOpen,
        'compression_mode'  => $compressionMode,
      ]
    ]);
    return fopen('snappy://'.$stream, $mode, false, $context);
  }

  /**
   * [public description]
   * @var resource
   */
  public $context;

  /**
   * [register description]
   */
  public static function register(): void {
    $wrapperExists = in_array("snappy", stream_get_wrappers());
    if ($wrapperExists) {
        // stream_wrapper_unregister("snappy");
    } else {
      stream_wrapper_register('snappy', static::class);
    }
  }

  /**
   * inner stream
   * @var resource
   */
  protected $parent = null;

  /**
   * Whether to leave the underlying stream open on stream close
   * @var bool
   */
  protected $leaveOpen = false;

  /**
   * @inheritDoc
   */
  public function __construct()
  {
    // throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function __destruct()
  {
    // throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function dir_closedir(): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function dir_opendir(string $path, int $options): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function dir_readdir(): string
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function mkdir(string $path, int $mode, int $options): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function rename(string $path_from, string $path_to): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function dir_rewinddir(): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function rmdir(string $path, int $options): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_cast(int $cast_as)
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function url_stat(string $path, int $flags): array
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function unlink(string $path): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_write(string $data): int
  {
    return fwrite($this->ms, $data);
  }

  /**
   * @inheritDoc
   */
  public function stream_truncate(int $new_size): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_tell(): int
  {
    return ftell($this->ms);
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * [protected description]
   * @var bool
   */
  protected $finishedForWriting = false;

  // /**
  //  * [MarkWriteFinished description]
  //  * @param [type] $resource [description]
  //  */
  // public static function MarkWriteFinished($resource): void
  // {
  //   //
  //   // check if resource is a snappy-resource
  //   // luckily, we can easily get the current instance via wrapper_data
  //   // and directly access some underyling stuff
  //   //
  //   $meta = stream_get_meta_data($resource);
  //   if(($wrapperInstance = $meta['wrapper_data'] ?? null) instanceof self) {
  //
  //     if($wrapperInstance->finishedForWriting) return;
  //
  //     if($wrapperInstance->compressionMode === static::MODE_COMPRESS) {
  //       $uncompressedLength = fstat($wrapperInstance->ms)['size'];
  //       $compressed = snappy_compress($content = stream_get_contents($wrapperInstance->ms, $uncompressedLength, 0));
  //       $compressedSize = strlen($compressed);
  //       fseek($wrapperInstance->parent, 0);
  //       fwrite($wrapperInstance->parent, $compressed);
  //       fflush($wrapperInstance->parent);
  //     }
  //
  //     $wrapperInstance->finishedForWriting = true;
  //   }
  //
  // }

  /**
   * @inheritDoc
   */
  public function MarkWriteFinished(): void
  {
    if($this->finishedForWriting) return;

    if($this->compressionMode === static::MODE_COMPRESS) {
      $uncompressedLength = fstat($this->ms)['size'];
      $compressed = snappy_compress(stream_get_contents($this->ms, $uncompressedLength, 0));
      $compressedSize = strlen($compressed);
      fseek($this->parent, 0);
      fwrite($this->parent, $compressed);
      fflush($this->parent);
    }

    $this->finishedForWriting = true;
  }


  /**
   * @inheritDoc
   */
  public function stream_stat(): array
  {
    return fstat($this->parent);
  }

  /**
   * @inheritDoc
   */
  public function stream_set_option(int $option, int $arg1, int $arg2): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_seek(int $offset, int $whence = SEEK_SET): bool
  {
    return fseek($this->ms, $offset, $whence) === 0;
    // return true;
    // throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_read(int $count): string
  {
    return fread($this->ms, $count);
  }

  protected static $prefix = 'snappy://';

  /**
   * [protected description]
   * @var int
   */
  protected $compressionMode;

  /**
   * [protected description]
   * @var resource
   */
  protected $ms;

  /**
   * @inheritDoc
   */
  public function stream_open(
    string $path,
    string $mode,
    int $options,
    ?string &$opened_path
  ): bool {
    if(strpos($path, 'Resource id #') === strlen(static::$prefix)) {
      // stringified resource identifier, get as handle via get_resource
      $resourceId = explode('#', $path)[1];
      $this->parent = get_resources()[$resourceId];

      // Passed via stream option
      $this->leaveOpen = stream_context_get_options($this->context)['snappy']['leave_open'] ?? false;
      $this->compressionMode = stream_context_get_options($this->context)['snappy']['compression_mode'] ?? null;

      if($this->compressionMode === null) {
        throw new Exception('Compression mode undefined');
      }

      if($this->compressionMode === static::MODE_COMPRESS) {
        $this->ms = fopen('php://memory', 'r+');
      } else {
        $this->ms = $this->DecompressFromStream($this->parent);
      }
    } else {
      // non-resource, manually fopen?
      throw new Exception('unsupported');
    }

    return true;
  }

  /**
   * [MODE_DECOMPRESS description]
   * @var int
   */
  const MODE_DECOMPRESS = 0;

  /**
   * [MODE_COMPRESS description]
   * @var int
   */
  const MODE_COMPRESS = 1;

  /**
   * [DecompressFromStream description]
   * @param  resource   $source [description]
   * @return resource        [description]
   */
  protected function DecompressFromStream($source)
  {
    $content = stream_get_contents($source);
    $decompressed = snappy_uncompress($content);
    $handle = fopen('php://memory', 'r+');
    fwrite($handle, $decompressed);
    fflush($handle);
    fseek($handle, 0);
    return $handle;

    // byte[] buffer = ArrayPool<byte>.Shared.Rent((int)source.Length);
    // int read = source.Read(buffer, 0, (int)source.Length);
    // byte[] uncompressedBytes = snappyDecompressor.Decompress(buffer, 0, (int)source.Length);
    // ArrayPool<byte>.Shared.Return(buffer);
    // return new MemoryStream(uncompressedBytes);
  }

  /**
   * @inheritDoc
   */
  public function stream_metadata(
    string $path,
    int $option,
    $value
  ): bool {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_lock(int $operation): bool
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_flush(): bool
  {
    return false;
    // TODO: only allow flush, if not markwritefinished?
    // return true;
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function stream_eof(): bool
  {
    return feof($this->ms); // ? IDEA: maybe depending on mode?
    // return false;
    // throw new \LogicException('Not implemented'); // TODO
    // return feof($this->parent);
  }

  /**
   * @inheritDoc
   */
  public function stream_close(): void
  {
    // NOT SURE ABOUT THIS THING.
    if(!$this->leaveOpen) {
      if(is_resource($this->parent)) {
        fclose($this->parent);
      } else {
        // Resource/stream already closed/disposed
        // QUESTION: Should we throw an exception?
        // throw new Exception('Inner stream already closed or freed');
      }
    }

    // QUESTION should we do this here?
    // We should kick out the reference for the GC to work...
    // NOTE/SEE: https://stackoverflow.com/questions/28195655/php-garbage-collection-when-using-static-method-to-create-instance
    // Different behaviour regarding resources in PHP than regular variables/members.
    // This seems to work for now, but I'm unsure about using WeakRef (pecl ext) or WeakReference (PHP 7.4+) in favor.
    //
    $this->parent = null;
  }
}
