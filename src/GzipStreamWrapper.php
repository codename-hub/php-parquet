<?php
namespace jocoon\parquet;

use Exception;


/**
 * [GzipStreamWrapper description]
 */
class GzipStreamWrapper implements StreamWrapperInterface
{
  /**
   * The internal default compression level
   * 1 = Lowest compression, highest speed
   * 6 = ZLIB/Gzip common default (usually)
   * 9 = 'Highest' compression, lowest speed
   * @var int
   */
  const DEFAULT_COMPRESSION_LEVEL = 6;

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
      'gzip' => [
        'leave_open'        => $leaveOpen,
        'compression_mode'  => $compressionMode,
        'compression_level'  => static::DEFAULT_COMPRESSION_LEVEL,
      ]
    ]);
    return fopen('gzip://'.$stream, $mode, false, $context);
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
    $wrapperExists = in_array("gzip", stream_get_wrappers());
    if ($wrapperExists) {
        // stream_wrapper_unregister("gzip");
    } else {
      stream_wrapper_register('gzip', static::class);
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
   * GZ/ZLIB compression level
   * @var int
   */
  protected $compressionLevel = 9;

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

  protected static $prefix = 'gzip://';

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
      $this->leaveOpen = stream_context_get_options($this->context)['gzip']['leave_open'] ?? false;
      $this->compressionMode = stream_context_get_options($this->context)['gzip']['compression_mode'] ?? null;
      $this->compressionLevel = stream_context_get_options($this->context)['gzip']['compression_level'] ?? 6; // Default fallback: 6 (common default)

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
    $decompressed = zlib_decode($content);
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
    if($this->compressionMode === static::MODE_COMPRESS) {
      $uncompressedLength = fstat($this->ms)['size'];

      //
      // These three are all valid and compatible choices for compression.
      // gzcompress seems to be a liiiiittle bit faster than zlib_encode, but its hard to notice
      // and detect, while not being good to be reproduced. Therefore: we stay with zlib_encode right now.
      // TODO: select the best approach by benchmarking and/or automatic detection?
      //
      $compressed = zlib_encode(stream_get_contents($this->ms, $uncompressedLength, 0), ZLIB_ENCODING_GZIP, $this->compressionLevel);
      // $compressed = gzcompress(stream_get_contents($this->ms, $uncompressedLength, 0), $this->compressionLevel, ZLIB_ENCODING_GZIP);
      // $compressed = gzencode(stream_get_contents($this->ms, $uncompressedLength, 0), $this->compressionLevel, FORCE_GZIP);

      $compressedSize = strlen($compressed);
      fseek($this->parent, 0);
      fwrite($this->parent, $compressed);
      fflush($this->parent);
      //
      // TODO: some error handling
      //
    }
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
