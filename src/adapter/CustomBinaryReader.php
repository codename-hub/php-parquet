<?php
namespace jocoon\parquet\adapter;

use Nelexa\Buffer\Cast;

class CustomBinaryReader extends BinaryReader
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    if(!is_resource($stream)) {
      //
      // This adapter relies on the concept of always working with a stream
      // Therefore, write into memory.
      //
      $this->stream = fopen('php://memory', 'r+');
      fwrite($this->stream, $stream);
    } else {
      $this->stream = $stream;
    }

    fseek($this->stream, 0);

    // Set LE by default
    $this->setByteOrder(static::LITTLE_ENDIAN);

    // TODO: options
  }

  /**
   * [protected description]
   * @var bool
   */
  protected $orderLittleEndian = false;

  /**
   * [setByteOrder description]
   * @param [type] $order [description]
   */
  public function setByteOrder($order) {
    $this->byteOrder = $order;
    $this->orderLittleEndian = $this->byteOrder === static::LITTLE_ENDIAN;
  }

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
   * [protected description]
   * @var resource
   */
  protected $stream;

  /**
   * @inheritDoc
   */
  public function isEof(): bool
  {
    return feof($this->stream);
  }

  /**
   * @inheritDoc
   */
  public function readBits($count)
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readUBits($count)
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readBytes($count)
  {
    return fread($this->stream, $count);
    // NOTE: we might REALLY parse bytes here instead of returning the read data as string
  }

  /**
   * @inheritDoc
   */
  public function readInt8()
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readUInt8()
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readInt16()
  {
    return Cast::toShort(unpack($this->orderLittleEndian ? 'v' : 'n', fread($this->stream, 2))[1]);
  }

  /**
   * @inheritDoc
   */
  public function readUInt16()
  {
    return unpack($this->orderLittleEndian ? 'v' : 'n', fread($this->stream, 2))[1];
  }

  /**
   * @inheritDoc
   */
  public function readInt32()
  {
    return Cast::toInt(unpack($this->orderLittleEndian ? 'V' : 'N', fread($this->stream, 4))[1]);
  }

  /**
   * @inheritDoc
   */
  public function readUInt32()
  {
    return unpack($this->orderLittleEndian ? 'V' : 'N', fread($this->stream, 4))[1];
  }

  /**
   * @inheritDoc
   */
  public function readInt64()
  {
    return unpack($this->orderLittleEndian ? 'P' : 'J', fread($this->stream, 8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readUInt64()
  {
    // Nelexa doesn't support reading unsigned longs
    throw new \LogicException('Not supported');
  }

  /**
   * @inheritDoc
   */
  public function readSingle()
  {
    return unpack($this->orderLittleEndian ? 'g' : 'G', fread($this->stream, 4))[1];
  }

  /**
   * @inheritDoc
   */
  public function readDouble()
  {
    return unpack($this->orderLittleEndian ? 'e' : 'E', fread($this->stream, 8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readString($length)
  {
    return fread($this->stream, $length);
  }

  /**
   * @inheritDoc
   */
  public function setInputHandle($inputHandle)
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function getInputHandle()
  {
    return $this->stream;
  }

  /**
   * @inheritDoc
   */
  public function setPosition(int $position)
  {
    fseek($this->stream, $position);
  }

  /**
   * @inheritDoc
   */
  public function getPosition(): int
  {
    return ftell($this->stream); // might be cached?
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    return fstat($this->stream)['size']; // might be cached
  }
}
