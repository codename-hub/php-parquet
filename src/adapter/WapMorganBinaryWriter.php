<?php
namespace jocoon\parquet\adapter;

class WapMorganBinaryWriter extends BinaryWriter
{
  /**
   * @inheritDoc
   */
  public function __construct($stream, $options = null)
  {
    if(!is_resource($stream)) {
      //
      // NOTE: this is utterly slow.
      //
      $s = fopen('php://memory', 'r+');
      fwrite($s, $stream);
      $stream = $s;
    }
    $this->stream = $stream;
    $this->binaryStream = new \wapmorgan\BinaryStream\BinaryStream($this->stream, \wapmorgan\BinaryStream\BinaryStream::REWRITE);

    // Set LE by default
    $this->binaryStream->setEndian(\wapmorgan\BinaryStream\BinaryStream::LITTLE);

    // TODO: options
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

    $this->binaryStream->writeString(call_user_func_array('pack', array_merge(['c*'], $value))); // ?????
  }

  /**
   * @inheritDoc
   */
  public function writeByte($value): void
  {
    $this->binaryStream->writeChar($value);
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
    $this->binaryStream->writeInteger($value, 16);
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
    $this->binaryStream->writeInteger($value, 32);
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
    $this->binaryStream->writeInteger($value, 64);
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
    $this->binaryStream->writeFloat($value, 32);
  }

  /**
   * @inheritDoc
   */
  public function writeDouble($value): void
  {
    $this->binaryStream->writeFloat($value, 64);
  }

  /**
   * @inheritDoc
   */
  public function writeString($value): void
  {
    $this->binaryStream->writeString($value);
  }

  /**
   * @inheritDoc
   */
  public function setPosition(int $position)
  {
    $this->binaryStream->go($position);
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

  /**
   * @inheritDoc
   */
  public function toString(): string
  {
    return stream_get_contents($this->stream);
  }
}
