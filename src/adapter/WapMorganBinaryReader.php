<?php
namespace jocoon\parquet\adapter;

/**
 * Implementation for wapmorgan/binary-stream
 */
class WapMorganBinaryReader extends BinaryReader
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
      fseek($this->stream, 0);
    } else {
      $this->stream = $stream;
    }
    $this->binaryStream = new \wapmorgan\BinaryStream\BinaryStream($this->stream);
    // TODO: options
  }

  /**
   * @inheritDoc
   */
  public function setInputString($inputString)
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * [protected description]
   * @var resource
   */
  protected $stream;

  /**
   * [protected description]
   * @var \wapmorgan\BinaryStream\BinaryStream
   */
  protected $binaryStream = null;

  /**
   * @inheritDoc
   */
  public function isEof(): bool
  {
    return $this->binaryStream->isEnd();
  }

  /**
   * @inheritDoc
   */
  public function readBits($count)
  {
    return $this->binaryStream->readBits($count);
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
    return $this->binaryStream->readString($count);
  }

  /**
   * @inheritDoc
   */
  public function readInt8()
  {
    return $this->binaryStream->readInteger(8);
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
    return $this->binaryStream->readInteger(16);
  }

  /**
   * @inheritDoc
   */
  public function readUInt16()
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readInt32()
  {
    return $this->binaryStream->readInteger(32);
  }

  /**
   * @inheritDoc
   */
  public function readUInt32()
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readInt64()
  {
    return $this->binaryStream->readInteger(64);
  }

  /**
   * @inheritDoc
   */
  public function readUInt64()
  {
    throw new \LogicException('Not implemented');
  }

  /**
   * @inheritDoc
   */
  public function readSingle()
  {
    return $this->binaryStream->readFloat(32);
  }

  /**
   * @inheritDoc
   */
  public function readDouble()
  {
    return $this->binaryStream->readFloat(64);
  }

  /**
   * @inheritDoc
   */
  public function readString($length)
  {
    return $this->binaryStream->readString($length);
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
    return $this->binaryStream->go($position);
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
    return fstat($this->stream)['size'];
  }
}
