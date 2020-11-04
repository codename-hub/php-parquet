<?php
namespace jocoon\parquet\adapter;

use Nelexa\Buffer\Buffer;

class NelexaBufferBinaryReader extends BinaryReader
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    if(!is_resource($stream)) {
      // //
      // // This adapter relies on the concept of always working with a stream
      // // Therefore, write into memory.
      // //
      // $this->stream = fopen('php://memory', 'r+');
      // fwrite($this->stream, $stream);
      // fseek($this->stream, 0);

      //
      // We're using Nelexa's specialty of a StringBuffer, avoiding stream initialisation.
      // NOTE: we are always relying on $stream being a string in this case and not a filepath.
      //
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
  public function isEof(): bool
  {
    return $this->buffer->position() >= $this->buffer->size();
    // return ftell($this->stream) >= fstat($this->stream)['size']; // feof($this->stream); // TODO: check if this really works.
  }

  /**
   * @inheritDoc
   */
  public function readBits($count)
  {
    // nelexa doesn't support Bit reading
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readUBits($count)
  {
    // nelexa doesn't support Bit reading
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readBytes($count)
  {
    return $this->buffer->getString($count);
  }

  /**
   * @inheritDoc
   */
  public function readInt8()
  {
    return $this->buffer->getByte();
  }

  /**
   * @inheritDoc
   */
  public function readUInt8()
  {
    return $this->buffer->getUnsignedByte();
  }

  /**
   * @inheritDoc
   */
  public function readInt16()
  {
    return $this->buffer->getShort();
  }

  /**
   * @inheritDoc
   */
  public function readUInt16()
  {
    return $this->buffer->getUnsignedShort();
  }

  /**
   * @inheritDoc
   */
  public function readInt32()
  {
    return $this->buffer->getInt();
  }

  /**
   * @inheritDoc
   */
  public function readUInt32()
  {
    return $this->buffer->getUnsignedInt();
  }

  /**
   * @inheritDoc
   */
  public function readInt64()
  {
    return $this->buffer->getLong();
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
    return $this->buffer->getFloat();
  }

  /**
   * @inheritDoc
   */
  public function readDouble()
  {
    return $this->buffer->getDouble();
  }

  /**
   * @inheritDoc
   */
  public function readString($length)
  {
    return $this->buffer->getString($length);
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
  public function setInputString($inputString)
  {
    throw new \LogicException('Not implemented'); // TODO
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
    return $this->buffer->setPosition($position);
  }

  /**
   * @inheritDoc
   */
  public function getPosition(): int
  {
    return $this->buffer->position(); // ftell($this->stream);
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    if($this->stream) {
      return fstat($this->stream)['size'];
    } else {
      return $this->buffer->size();
    }
  }
}
