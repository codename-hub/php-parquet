<?php
namespace jocoon\parquet\adapter;

/**
 * Adapter for mdurrant's PhpBinaryReader
 */
class PhpBinaryReader extends BinaryReader
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    $this->binaryReader = new \PhpBinaryReader\BinaryReader($stream);
    // TODO: options
  }

  /**
   * [protected description]
   * @var \PhpBinaryReader\BinaryReader
   */
  protected $binaryReader = null;

  /**
   * @inheritDoc
   */
  public function isEof(): bool
  {
    return $this->binaryReader->isEof();
  }

  /**
   * @inheritDoc
   */
  public function readBits($count)
  {
    return $this->binaryReader->readBits($count);
  }

  /**
   * @inheritDoc
   */
  public function readUBits($count)
  {
    return $this->binaryReader->readUBits($count);
  }

  /**
   * @inheritDoc
   */
  public function readBytes($count)
  {
    return $this->binaryReader->readBytes($count);
  }

  /**
   * @inheritDoc
   */
  public function readInt8()
  {
    return $this->binaryReader->readInt8();
  }

  /**
   * @inheritDoc
   */
  public function readUInt8()
  {
    return $this->binaryReader->readUInt8();
  }

  /**
   * @inheritDoc
   */
  public function readInt16()
  {
    return $this->binaryReader->readInt16();
  }

  /**
   * @inheritDoc
   */
  public function readUInt16()
  {
    return $this->binaryReader->readUInt16();
  }

  /**
   * @inheritDoc
   */
  public function readInt32()
  {
    return $this->binaryReader->readInt32();
  }

  /**
   * @inheritDoc
   */
  public function readUInt32()
  {
    return $this->binaryReader->readUInt32();
  }

  /**
   * @inheritDoc
   */
  public function readInt64()
  {
    return $this->binaryReader->readInt64();
  }

  /**
   * @inheritDoc
   */
  public function readUInt64()
  {
    return $this->binaryReader->readUInt64();
  }

  /**
   * @inheritDoc
   */
  public function readSingle()
  {
    return $this->binaryReader->readSingle();
  }

  /**
   * @inheritDoc
   */
  public function readDouble()
  {
    return \unpack($this->binaryReader->getEndian() === \PhpBinaryReader\Endian::LITTLE  ? 'e' : 'E', $this->binaryReader->readBytes(8))[1];
  }

  /**
   * @inheritDoc
   */
  public function readString($length)
  {
    return $this->binaryReader->readString($length);
  }

  /**
   * @inheritDoc
   */
  public function setInputHandle($inputHandle)
  {
    $this->binaryReader->setInputHandle($inputHandle);
  }

  /**
   * @inheritDoc
   */
  public function setInputString($inputString)
  {
    $this->binaryReader->setInputString($inputString);
  }

  /**
   * @inheritDoc
   */
  public function getInputHandle()
  {
    return $this->binaryReader->getInputHandle();
  }

  /**
   * @inheritDoc
   */
  public function setPosition(int $position)
  {
    return $this->binaryReader->setPosition($position);
  }

  /**
   * @inheritDoc
   */
  public function getPosition(): int
  {
    return $this->binaryReader->getPosition();
  }

  /**
   * @inheritDoc
   */
  public function getEofPosition(): int
  {
    return $this->binaryReader->getEofPosition();
  }
}
