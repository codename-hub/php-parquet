<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

class StringDataTypeHandler extends BasicDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'string';
    parent::__construct(DataType::String, Type::BYTE_ARRAY, ConvertedType::UTF8);
  }

  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
  ): bool {

    return $tse->type && $tse->type === Type::BYTE_ARRAY &&
      (
        ($tse->converted_type !== null && $tse->converted_type === ConvertedType::UTF8) ||
        $formatOptions->TreatByteArrayAsString
      );
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    if ($length === -1) $length = $reader->readInt32();

    // NOTE: possible UTF8 handling needed.
    return $reader->readString($length);
  }

  /**
   * @inheritDoc
   */
  public function read(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    array &$dest,
    int $offset
  ): int {

    $remLength = (int)($reader->getEofPosition() - $reader->getPosition());

    if ($remLength === 0) {
      return 0;
    }

    // reading string one by one is extremely slow, read all data
    $allBytes = $reader->readBytes($remLength);

    $destIdx = $offset;
    $spanIdx = 0;
    $destCount = \count($dest);

    while ($spanIdx < $remLength && $destIdx < $destCount)
    {
      // see https://www.php.net/manual/de/function.unpack.php
      // unpack("l", $value)[1]  is for int32
      $length = \unpack('l', \substr($allBytes, $spanIdx, 4))[1];
      $s = \substr($allBytes, $spanIdx + 4, $length);

      $dest[$destIdx++] = $s;
      $spanIdx = $spanIdx + 4 + $length;
    }

    return $destIdx - $offset;
  }

  /**
   * @inheritDoc
   */
  public function unpackDefinitions(
    array $src,
    array $definitionLevels,
    int $maxDefinitionLevel,
    array &$hasValueFlags
  ): array {
    return $this->unpackGenericDefinitions($src, $definitionLevels, $maxDefinitionLevel, $hasValueFlags);
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\jocoon\parquet\adapter\BinaryWriter $writer, $value): void
  {
    $valueLength = null;
    if ($value === null || ($valueLength = \strlen($value)) === 0)
    {
      $writer->writeInt32(0);
    }
    else
    {
      // transform to byte array first, as we need the length of the byte buffer, not string length
      // byte[] data = E.GetBytes(value);
      // NOTE: for php, we already have binary-safe functions, so we simply write a string.
      $valueLength = $valueLength ?? \strlen($value);
      $writer->writeInt32($valueLength);
      $writer->writeString($value);
    }
  }

}
