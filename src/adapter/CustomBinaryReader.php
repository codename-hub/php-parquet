<?php
namespace codename\parquet\adapter;

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
      // $this->stream = fopen('php://memory', 'r+');
      // fwrite($this->stream, $stream);
      $this->setInputString($stream);
    } else {
      $this->stream = $stream;
    }


    fseek($this->stream, 0);
    $this->position = 0;
    $this->size = fstat($this->stream)['size'];

    // Set LE by default
    $this->setByteOrder(static::LITTLE_ENDIAN);

    // TODO: options
  }

  /**
   * [protected description]
   * @var int
   */
  protected $size = null;

  /**
   * [protected description]
   * @var int
   */
  protected $position = null;

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
    $this->position += $count;
    return $count > 0 ? fread($this->stream, $count) : ''; // Same behaviour as BinaryReader
    // NOTE: we might REALLY parse bytes here instead of returning the read data as string
  }

  /**
   * @inheritDoc
   */
  public function readByte()
  {
    return unpack('c', fread($this->stream, 1))[1];
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
    $this->position += 2;
    return Cast::toShort(unpack($this->orderLittleEndian ? 'v' : 'n', fread($this->stream, 2))[1]);
  }

  /**
   * @inheritDoc
   */
  public function readUInt16()
  {
    $this->position += 2;
    return unpack($this->orderLittleEndian ? 'v' : 'n', fread($this->stream, 2))[1];
  }

  /**
   * @inheritDoc
   */
  public function readInt32()
  {
    $this->position += 4;
    return Cast::toInt(unpack($this->orderLittleEndian ? 'V' : 'N', fread($this->stream, 4))[1]);
  }

  /**
   * @inheritDoc
   */
  public function readUInt32()
  {
    $this->position += 4;
    return unpack($this->orderLittleEndian ? 'V' : 'N', fread($this->stream, 4))[1];
  }

  /**
   * @inheritDoc
   */
  public function readInt64()
  {
    $this->position += 8;
    return unpack($this->orderLittleEndian ? 'P' : 'J', fread($this->stream, 8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readUInt64()
  {
    // Nelexa doesn't support reading unsigned longs, PHP doesn't either.
    // But for the sake of compatibility, we try to read it somehow.
    // Actually, we'd have to check for Int64 overflow
    // throw new \LogicException('Not supported');
    $this->position += 8;
    return unpack($this->orderLittleEndian ? 'P' : 'J', fread($this->stream, 8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readSingle()
  {
    $this->position += 4;
    return unpack($this->orderLittleEndian ? 'g' : 'G', fread($this->stream, 4))[1];
  }

  /**
   * @inheritDoc
   */
  public function readDouble()
  {
    $this->position += 8;
    return unpack($this->orderLittleEndian ? 'e' : 'E', fread($this->stream, 8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readString($length)
  {
    $this->position += $length; // ?
    return $length > 0 ? fread($this->stream, $length) : '';
  }

  /**
   * @inheritDoc
   */
  public function setInputHandle($inputHandle)
  {
    $this->stream = $inputHandle;
  }

  /**
   * [setInputString description]
   * @param [type] $inputString [description]
   */
  public function setInputString($inputString) {
    $this->stream = fopen('php://memory', 'r+');
    fwrite($this->stream, $inputString);
    fseek($this->stream, 0);
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
    $this->size = ftell($this->stream);
  }

  /**
   * @inheritDoc
   */
  public function getPosition(): int
  {
    return $this->position; // ftell($this->stream); // might be cached?
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    return $this->size; // fstat($this->stream)['size']; // might be cached
  }
}
