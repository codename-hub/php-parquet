<?php
namespace jocoon\parquet;

use Exception;

/**
 * [GapStreamWrapper description]
 */
class GapStreamWrapper implements StreamWrapperInterface, MarkWriteFinishedInterface
{
  /**
   * [createWrappedStream description]
   * @param resource  $stream       [description]
   * @param string    $mode         [description]
   * @param int|null  $knownLength  [known length of final stream]
   * @param bool      $leaveOpen    [whether to leave underlying stream open on closing of the gap stream]
   * @return resource
   */
  public static function createWrappedStream($stream, $mode, ?int $knownLength = null, bool $leaveOpen = false) {
    $context = stream_context_create([
      'gap' => [
        'leave_open'    => $leaveOpen,
        'known_length'  => $knownLength,
      ]
    ]);
    return fopen('gap://'.$stream, $mode, false, $context);
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
    $wrapperExists = in_array("gap", stream_get_wrappers());
    if ($wrapperExists) {
        // stream_wrapper_unregister("gap");
    } else {
      stream_wrapper_register('gap', static::class);
    }
  }

  /**
   * parent stream
   * @var resource
   */
  protected $parent = null;

  /**
   * [protected description]
   * @var int|null
   */
  protected $knownLength = null;

  /**
   * Whether to leave the underlying stream open on stream close
   * @var bool
   */
  protected $leaveOpen = false;

  /**
   * [protected description]
   * @var int|null
   */
  protected $position;

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
    // echo("GapStream::WRITE @ ".ftell($this->parent)." >> ".$data." <<  ".chr(10));
    $written = \fwrite($this->parent, $data);
    // echo("GapStream::WRITE written + ".$written.chr(10));
    $this->position += $written;
    return $written;
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
    return $this->position;
  }

  /**
   * @inheritDoc
   */
  public function stream_stat(): array
  {
    // return fstat($this->parent);

    //
    // see https://www.php.net/manual/en/function.stat.php
    //
    return [
      // TODO: more fields?
      7       => $this->knownLength ?? fstat($this->parent)[7],
      'size'  => $this->knownLength ?? fstat($this->parent)['size'],
    ];
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
    $success = fseek($this->parent, $offset, $whence);
    // echo("Gapstream seek $offset $whence $retval");
    // DEBUG
    // $trace = debug_backtrace();
    // foreach($trace as $te) {
    //   echo("TRACE ".$te['file'].':'.$te['line'].chr(10));
    // }
    // print_r($trace);

    // TODO: handle errors
    $this->position = ftell($this->parent);
    return $success === 0;
  }

  /**
   * @inheritDoc
   */
  public function stream_read(int $count): string
  {
    $readValue = fread($this->parent, $count);
    $this->position += strlen($readValue);
    return $readValue;
  }

  /**
   * @inheritDoc
   */
  public function stream_open(
    string $path,
    string $mode,
    int $options,
    ?string &$opened_path
  ): bool {
    if(strpos($path, 'Resource id #') === 6) {

      // stringified resource identifier, get as handle via get_resource
      $resourceId = explode('#', $path)[1];
      $this->parent = get_resources()[$resourceId];

      // Passed via stream option
      $this->leaveOpen = stream_context_get_options($this->context)['gap']['leave_open'] ?? false;
      $this->knownLength = stream_context_get_options($this->context)['gap']['known_length'] ?? null;

    } else {
      // non-resource, manually fopen?
      throw new Exception('unsupported');
    }

    return true;
  }

  /**
   * @inheritDoc
   */
  public function MarkWriteFinished(): void
  {
    $innerMeta = stream_get_meta_data($this->parent);
    if(($innerWrapperInstance = $innerMeta['wrapper_data'] ?? null) instanceof MarkWriteFinishedInterface) {
      $innerWrapperInstance->MarkWriteFinished();
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
    return fflush($this->parent);
  }

  /**
   * @inheritDoc
   */
  public function stream_eof(): bool
  {
    //
    // As far as we have a knownLength, compare it to our current 'position'
    // Otherwise, fallback to parent stream's EOF state
    //
    return $this->knownLength ? ($this->position >= $this->knownLength) : feof($this->parent);
  }

  /**
   * @inheritDoc
   */
  public function stream_close(): void
  {
    // echo("stream close of gap stream!".chr(10));

    // print_r(\jocoon\parquet\GapStreamWrapper::$staticParents);
    //
    // $trace = debug_backtrace();
    // foreach($trace as $te) {
    //   echo("TRACE ".$te['file'].':'.$te['line'].chr(10));
    // }

    // NOT SURE ABOUT THIS THING.
    if(!$this->leaveOpen) {
      if(is_resource($this->parent)) {
        fclose($this->parent);
      } else {
        // Resource/stream already closed/disposed
        // QUESTION: Should we throw an exception?
        // echo("**** stream close without parent ****");
        // print_r($this->parent);
        // print_r(fstat($this->parent));
        // $trace = debug_backtrace();
        // foreach($trace as $te) {
        //   echo("{$te['file']}:{$te['line']}".chr(10));
        // }
        throw new Exception('Parent stream already closed or freed');
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
