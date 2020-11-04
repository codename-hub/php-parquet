<?php
namespace jocoon\parquet\adapter;

/**
 *
 */
interface BinaryReaderInterface
{
  /**
   * [__construct description]
   * @param [type] $stream  [description]
   * @param [type] $options [description]
   */
  public function __construct($stream, $options = null);

  /**
   * @return bool
   */
  public function isEof(): bool;

  /**
   * @param  int $count
   * @return int
   */
  public function readBits($count);

  /**
   * @param  int $count
   * @return int
   */
  public function readUBits($count);

  /**
   * @param  int $count
   * @return int
   */
  public function readBytes($count);

  /**
   * @return int
   */
  public function readInt8();

  /**
   * @return int
   */
  public function readUInt8();

  /**
   * @return int
   */
  public function readInt16();

  /**
   * @return string
   */
  public function readUInt16();

  /**
   * @return int
   */
  public function readInt32();

  /**
   * @return int
   */
  public function readUInt32();

  /**
   * @return int
   */
  public function readInt64();

  /**
   * @return int
   */
  public function readUInt64();

  /**
   * @return float
   */
  public function readSingle();

  /**
   * @return float
   */
  public function readDouble();

  /**
   * @param  int    $length
   * @return string
   */
  public function readString($length);

  /**
   * @param  resource $inputHandle
   */
  public function setInputHandle($inputHandle);

  /**
   * @param  string $inputString
   */
  public function setInputString($inputString);

  /**
   * @return resource
   */
  public function getInputHandle();

  /**
   * @param  int   $position
   */
  public function setPosition(int $position);

  /**
   * @return int
   */
  public function getPosition(): int;

  /**
   * @return int
   */
  public function getEofPosition(): int;
}
