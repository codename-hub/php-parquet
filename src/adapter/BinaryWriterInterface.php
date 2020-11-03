<?php
namespace jocoon\parquet\adapter;

/**
 *
 */
interface BinaryWriterInterface
{
  /**
   * [__construct description]
   * @param [type] $stream  [description]
   * @param [type] $options [description]
   */
  public function __construct($stream, $options = null);

  /**
   * [getBaseStream description]
   * @return resource [description]
   */
  public function getBaseStream();

  /**
   * [writeBits description]
   * @param [type] $value [description]
   */
  public function writeBits($value): void;

  /**
   * [writeUBits description]
   * @param [type] $value [description]
   */
  public function writeUBits($value): void;

  /**
   * [writeBytes description]
   * @param [type] $value [description]
   */
  public function writeBytes($value): void;

  /**
   * [writeByte description]
   * @param [type] $value [description]
   */
  public function writeByte($value): void;

  /**
   * [writeInt8 description]
   * @param [type] $value [description]
   */
  public function writeInt8($value): void;

  /**
   * [writeUInt8 description]
   * @param [type] $value [description]
   */
  public function writeUInt8($value): void;

  /**
   * [writeInt16 description]
   * @param [type] $value [description]
   */
  public function writeInt16($value): void;

  /**
   * [writeUInt16 description]
   * @param [type] $value [description]
   */
  public function writeUInt16($value): void;

  /**
   * [writeInt32 description]
   * @param [type] $value [description]
   */
  public function writeInt32($value): void;

  /**
   * [writeUInt32 description]
   * @param [type] $value [description]
   */
  public function writeUInt32($value): void;

  /**
   * [writeInt64 description]
   * @param [type] $value [description]
   */
  public function writeInt64($value): void;

  /**
   * [writeUInt64 description]
   * @param [type] $value [description]
   */
  public function writeUInt64($value): void;

  /**
   * Writes a float (32-bit)
   * @param  float $value [description]
   */
  public function writeSingle($value): void;

  /**
   * Writes a double (64-bit)
   * @param  double $value [description]
   */
  public function writeDouble($value): void;

  /**
   * Writes a string
   * @param string $value [description]
   */
  public function writeString($value): void;

  /**
   * @param  int   $position
   * @return $this
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

  /**
   * Contents of the BinaryWriter
   * @return string [description]
   */
  public function toString(): string;
}
