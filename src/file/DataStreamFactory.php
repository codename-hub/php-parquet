<?php
namespace jocoon\parquet\file;

use Exception;

use jocoon\parquet\GapStreamWrapper;
use jocoon\parquet\CompressionMethod;
use jocoon\parquet\GzipStreamWrapper;
use jocoon\parquet\SnappyInMemoryStreamWrapper;

use jocoon\parquet\format\CompressionCodec;

class DataStreamFactory
{

  /**
   * [codecToCompressionMethod description]
   * @var array
   */
  const codecToCompressionMethod = [
    CompressionCodec::UNCOMPRESSED  => 'none',
    CompressionCodec::GZIP          => 'gzip',
    CompressionCodec::SNAPPY        => 'snappy',
  ];

  /**
   * Registers required/available stream wrappers
   */
  public static function registerStreamWrappers(): void {
    GapStreamWrapper::register();
    GzipStreamWrapper::register();
    SnappyInMemoryStreamWrapper::register();
  }

  /**
   * [CreateWriter description]
   * @param resource  $nakedStream       [description]
   * @param int       $compressionMethod [description]
   * @param bool      $leaveNakedOpen    [description]
   * @return resource [handle of the writable (gap) stream]
   */
  public static function CreateWriter(
     $nakedStream, int $compressionMethod,
     bool $leaveNakedOpen)
  {
     $dest = null;

     switch($compressionMethod)
     {
        case CompressionMethod::Gzip:
          $dest = GzipStreamWrapper::createWrappedStream($nakedStream, 'r+', GzipStreamWrapper::MODE_COMPRESS, $leaveNakedOpen);
          $leaveNakedOpen = false;
          break;

        case CompressionMethod::Snappy:
          $dest = SnappyInMemoryStreamWrapper::createWrappedStream($nakedStream, 'r+', SnappyInMemoryStreamWrapper::MODE_COMPRESS);
          $leaveNakedOpen = false;
           break;

        case CompressionMethod::None:
           $dest = $nakedStream;
           break;

        default:
           throw new Exception("unknown compression method {$compressionMethod}");
     }

     $handle = GapStreamWrapper::createWrappedStream($dest, 'r+', null, $leaveNakedOpen);

     if(!is_resource($handle)) {
       throw new Exception('Handle is not a resource');
     }

     return $handle; // new GapStream(dest, leaveOpen: leaveNakedOpen);
  }

  /**
   * [ReadPageData description]
   * @param resource  $nakedStream        [description]
   * @param int       $compressionCodec   [description]
   * @param int       $compressedLength   [description]
   * @param int       $uncompressedLength [description]
   */
  public static function ReadPageData(
    $nakedStream, int $compressionCodec,
        int $compressedLength, int $uncompressedLength
  ) {
    $compressionMethod = static::codecToCompressionMethod[$compressionCodec] ?? null;
    if($compressionMethod === null) {
      throw new \Exception('not supported compressionCodec');
    }

    $totalBytesRead = 0;
    $currentBytesRead = PHP_INT_MIN;
    $data = '';
    // bytes pool?

    while(($totalBytesRead < $compressedLength) && ($currentBytesRead !== 0)) {
      $read = fread($nakedStream, $compressedLength - $totalBytesRead);
      $data .= $read;
      $currentBytesRead = strlen($read); // binary safety?
      $totalBytesRead += $currentBytesRead;
    }

    if($totalBytesRead != $compressedLength) {
      throw new Exception('unexpected byte amount');
    }

    switch($compressionMethod) {

      case 'none':
        //
        // No compression, just break out of switch.
        //
        break;

      case 'gzip':
        //
        // NOTE: for stream filters, we'd have to use window = 31
        // see https://gist.github.com/joelwurtz/c06bcfbb6766cb2b1d53
        //
        // It is important to say, stream filters _DO_NOT_ add headers or tail checksums
        // Which leaves a compressed stream unusable in these cases.
        //
        // You might be able to use gzencode or other functions, instead
        // But zlib automatically detects the respective encoding.
        //
        // TODO: Check performance/compare to other methods.
        //
        $uncompressedData = zlib_decode($data, $uncompressedLength);

        if($uncompressedData === false) {
          throw new Exception('Decompression error (gzip)');
        }

        $data = $uncompressedData;
        break;

      case 'snappy':
        //
        // NOTE: needs php-snappy / php-ext-snappy to be installed
        // You'll need to compile and install it yourself at the moment (no PECL and stuff).
        //
        $uncompressedData = snappy_uncompress($data);
        if($uncompressedData === false) {
          throw new \Exception('Decompression error (snappy)');
        }
        $data = $uncompressedData;
        break;

      default:
        throw new \Exception('unsupported compression method '. $compressionMethod);
    }

    return $data;
  }
}
