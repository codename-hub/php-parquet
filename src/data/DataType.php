<?php
namespace jocoon\parquet\data;

/**
 * [abstract description]
 */
abstract class DataType
{
  /**
   * [Unspecified description]
   * @var int
   */
  const Unspecified = 1;

  /**
   * [Boolean description]
   * @var int
   */
  const Boolean = 2;

  /**
   * [Byte description]
   * @var int
   */
  const Byte = 3;

  /**
   * [SignedByte description]
   * @var int
   */
  const SignedByte = 4;

  const UnsignedByte = 5;
  const Short = 6;
  const UnsignedShort = 7;

  const Int16 = 8;

  const UnsignedInt16 = 9;
  const Int32 = 10;
  const Int64 = 11;
  const Int96 = 12;

  const ByteArray = 13;

  const String = 14;

  const Float = 15;

  const Double = 16;

  const Decimal = 17;

  const DateTimeOffset = 18;

  const Interval = 19;
}
