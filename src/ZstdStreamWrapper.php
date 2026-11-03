<?php
namespace codename\parquet;

use Exception;


/**
 * [ZstdStreamWrapper description]
 */
class ZstdStreamWrapper implements StreamWrapperInterface, MarkWriteFinishedInterface
{
  /**
   * Default zstd compression level used when writing pages.
   *
   * zstd_compress accepts levels 1..22 (and negative values down to
   * ZSTD_minCLevel() for "fast" mode — roughly -131072..-1, ratio drops
   * sharply in exchange for very fast writes).
   *
   *   1   = fastest write, weakest ratio
   *   3   = upstream zstd default
   *   9   = chosen here: good ratio for parquet's per-page chunks without
   *         materially slowing writes (per benchmarking)
   *  15+ = noticeably better ratios on this workload, but write cost
   *         starts to climb (level 22 is ~30% slower than 9)
   *
   * Per-page compression means zstd can't build a dictionary across the
   * file, so very high levels show diminishing returns vs. one-shot
   * compression of the same data.
   *
   * @var int
   */
  const DEFAULT_COMPRESSION_LEVEL = 9;

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
      'zstd' => [
        'leave_open'        => $leaveOpen,
        'compression_mode'  => $compressionMode,
        'compression_level'  => static::DEFAULT_COMPRESSION_LEVEL,
      ]
    ]);
    return fopen('zstd://'.$stream, $mode, false, $context);
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
    $wrapperExists = in_array("zstd", stream_get_wrappers());
    if ($wrapperExists) {
        // stream_wrapper_unregister("gzip");
    } else {
      stream_wrapper_register('zstd', static::class);
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
   * Zstd compression level (1..22, or negative for fast mode).
   * Overwritten in stream_open from the stream context; this initializer
   * is only the fallback if the wrapper is somehow constructed without a
   * context, and matches {@see DEFAULT_COMPRESSION_LEVEL}.
   * @var int
   */
  protected $compressionLevel = self::DEFAULT_COMPRESSION_LEVEL;

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

  protected static $prefix = 'zstd://';

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
   * Flag to track if compression has been written
   * @var bool
   */
  protected $finishedForWriting = false;

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
      $this->leaveOpen = stream_context_get_options($this->context)['zstd']['leave_open'] ?? false;
      $this->compressionMode = stream_context_get_options($this->context)['zstd']['compression_mode'] ?? null;
      $this->compressionLevel = stream_context_get_options($this->context)['zstd']['compression_level'] ?? static::DEFAULT_COMPRESSION_LEVEL;

      if($this->compressionMode === null) {
        throw new Exception('Compression mode undefined');
      }

      if($this->compressionMode === static::MODE_COMPRESS) {
        $this->ms = fopen('php://memory', 'r+');
      } else {
        $this->ms = $this->decompressFromStream($this->parent);
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
   * [decompressFromStream description]
   * @param  resource   $source [description]
   * @return resource        [description]
   */
  protected function decompressFromStream($source)
  {
    $content = stream_get_contents($source);
    $decompressed = zstd_uncompress($content);
    $handle = fopen('php://memory', 'r+');
    fwrite($handle, $decompressed);
    fflush($handle);
    fseek($handle, 0);
    return $handle;
  }

  /**
   * Writes out the final, compressed stream
   */
  protected function writeCompressedStream(): void {
    if($this->finishedForWriting) return;

    if($this->compressionMode === static::MODE_COMPRESS) {
      $uncompressedLength = fstat($this->ms)['size'];

      $compressed = zstd_compress(stream_get_contents($this->ms, $uncompressedLength, 0), $this->compressionLevel);

      $compressedSize = strlen($compressed);
      fseek($this->parent, 0);
      fwrite($this->parent, $compressed);
      fflush($this->parent);
      //
      // TODO: some error handling
      //
    }

    $this->finishedForWriting = true;
  }

  /**
   * @inheritDoc
   */
  public function MarkWriteFinished(): void
  {
    $this->writeCompressedStream();
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
    //
    // TODO: check, what we should do here...
    // We might flush $this->ms
    //
    return false;
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
    //
    // Write out (compress!) on close
    // As long as not closed, we delay compression until closing of this stream.
    //
    $this->writeCompressedStream();

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
