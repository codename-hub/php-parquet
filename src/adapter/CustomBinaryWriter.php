<?php
namespace jocoon\parquet\adapter;

class CustomBinaryWriter extends BinaryWriter
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    if(!is_resource($stream)) {
      // Allow supplying a string as content - may it be empty or not
      // to avoid initializing php://memory with fopen

      $s = fopen('php://memory', 'r+');
      fwrite($s, $stream);
      $stream = $s;
    }

    fseek($stream, 0);

    $this->stream = $stream;

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
  public function getBaseStream()
  {
    return $this->stream;
  }

  /**
   * @inheritDoc
   */
  public function writeBits($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeUBits($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeBytes($value): void
  {
    fwrite($this->stream, call_user_func_array('pack', array_merge(['c*'], $value)));
  }

  /**
   * @inheritDoc
   */
  public function writeByte($value): void
  {
    fwrite($this->stream, pack('c', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeUInt8($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeInt16($value): void
  {

    fwrite($this->stream, pack($this->orderLittleEndian ? 'v' : 'n', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeUInt16($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeInt8($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeInt32($value): void
  {
    fwrite($this->stream, pack($this->orderLittleEndian ? 'V' : 'N', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeUInt32($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeInt64($value): void
  {
    fwrite($this->stream, pack($this->orderLittleEndian ? 'P' : 'J', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeUInt64($value): void
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function writeSingle($value): void
  {
    fwrite($this->stream, pack($this->orderLittleEndian ? 'g' : 'G', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeDouble($value): void
  {
    fwrite($this->stream, pack($this->orderLittleEndian ? 'e' : 'E', $value));
  }

  /**
   * @inheritDoc
   */
  public function writeString($value): void
  {
    fwrite($this->stream, $value);
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
    return ftell($this->stream);
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    return fstat($this->stream)['size']; // TODO: might be cached.
  }

  /**
   * @inheritDoc
   */
  public function toString(): string
  {
    fseek($this->stream, 0);
    return stream_get_contents($this->stream);
  }
}
