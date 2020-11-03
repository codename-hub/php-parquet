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

    // NOTE/TODO/QUESTION: unsure about this part - should be perform a utf8_decode or not?
    return $reader->readString($length); // UTF8?
    // return utf8_decode($reader->readString($length)); // UTF8?
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

    if ($remLength == 0) {
      return 0;
    }

    // $tdest = &$dest; // TODO/QUESTION: either this or clone $dest; ??

    // reading string one by one is extremely slow, read all data

    // byte[] allBytes = _bytePool.Rent(remLength);
    // $allBytes = []; // TODO: pre-fill or SplFixedArray ?

    // reader.BaseStream.Read(allBytes, 0, remLength);
    // $allBytes = $reader->readAlignedString($remLength);
    $allBytes = $reader->readBytes($remLength);

    $destIdx = $offset;

    try
    {
      // Span<byte> span = allBytes.AsSpan(0, remLength);   //will be passed as input in future versions
      // $span = $allBytes; // substr($allBytes, 0, $remLength);

      $spanIdx = 0;
      $spanLength = \strlen($allBytes);
      $destCount = \count($dest);

      while ($spanIdx < $spanLength && $destIdx < $destCount)
      {
        // $length = span.Slice(spanIdx, 4).ReadInt32();
        // $length = intval(substr($span, $spanIdx, 4));

        // see https://www.php.net/manual/de/function.unpack.php
        // unpack("l", $value)[1]  is for int32
        $length = \unpack("l", \substr($allBytes, $spanIdx, 4))[1];

        // string s = E.GetString(allBytes, spanIdx + 4, length);
        $s = \substr($allBytes, $spanIdx + 4, $length);

        // tdest[destIdx++] = s;
        $dest[$destIdx++] = $s;

        $spanIdx = $spanIdx + 4 + $length;
        // spanIdx = spanIdx + 4 + length;
      }
    }
    finally
    {
      // _bytePool.Return(allBytes);
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
    if ($value === null || ($valueLength = strlen($value)) === 0)
    {
      $writer->writeInt32(0);
    }
    else
    {
      //transofrm to byte array first, as we need the length of the byte buffer, not string length
      // byte[] data = E.GetBytes(value);
      // writer.Write(data.Length);
      // writer.Write(data);
      $valueLength = $valueLength ?? \strlen($value);
      $writer->writeInt32($valueLength);
      $writer->writeString($value);
    }
  }

}
