<?php
namespace jocoon\parquet\adapter;

use Nelexa\Buffer\Buffer;

class NelexaBufferBinaryWriter extends BinaryWriter
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    if(!is_resource($stream)) {
      // Allow supplying a string as content - may it be empty or not
      // to avoid initializing php://memory with fopen
      $this->buffer = new \Nelexa\Buffer\StringBuffer($stream);
    } else {
      $this->stream = $stream;
      $this->buffer = new \Nelexa\Buffer\ResourceBuffer($this->stream);
    }

    // Set LE by default
    $this->buffer->setOrder(Buffer::LITTLE_ENDIAN);
    // TODO: options
  }

  /**
   * [protected description]
   * @var resource
   */
  protected $stream;

  /**
   * [protected description]
   * @var Buffer
   */
  protected $buffer = null;

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
    $this->buffer->insertArrayBytes($value); // ??
  }

  /**
   * @inheritDoc
   */
  public function writeByte($value): void
  {
    $this->buffer->insertByte($value);
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
    $this->buffer->insertShort($value);
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
    $this->buffer->insertInt($value);
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
    $this->buffer->insertLong($value);
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
    $this->buffer->insertFloat($value);
  }

  /**
   * @inheritDoc
   */
  public function writeDouble($value): void
  {
    $this->buffer->insertDouble($value);
  }

  /**
   * @inheritDoc
   */
  public function writeString($value): void
  {
    $this->buffer->insertString($value);
  }

  /**
   * @inheritDoc
   */
  public function setPosition(int $position)
  {
    $this->buffer->setPosition($position);
  }

  /**
   * @inheritDoc
   */
  public function getPosition(): int
  {
    return $this->buffer->position();
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    return $this->buffer->size();
  }

  /**
   * @inheritDoc
   */
  public function toString(): string
  {
    return $this->buffer->toString();
  }
}
