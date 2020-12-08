<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicDataTypeHandler;

use jocoon\parquet\format\Type;

class ByteArrayDataTypeHandler extends BasicDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'array';
    parent::__construct(DataType::ByteArray, Type::BYTE_ARRAY);
  }

  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
  ): bool {
    return isset($tse->type)  && $tse->type === Type::BYTE_ARRAY
                              && !isset($tse->converted_type);
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

    // byte[][] tdest = (byte[][])dest;
    $tdest = &$dest;

    //reading byte[] one by one is extremely slow, read all data

    // $allBytes = []; _bytePool.Rent(remLength);

    $allBytes = $reader->readString($remLength);

    // var_dump($allBytes);
    // die();

    // reader.BaseStream.Read(allBytes, 0, remLength);
    $destIdx = $offset;
    try
    {
      $span = substr($allBytes, 0, $remLength);

      $spanIdx = 0;
      $spanLength = strlen($allBytes);

      while ($spanIdx < $spanLength && $destIdx < count($tdest))
      {
        // $length = span.Slice(spanIdx, 4).ReadInt32();
        // $length = intval(substr($span, $spanIdx, 4));

        // see https://www.php.net/manual/de/function.unpack.php
        // unpack("l", $value)[1]  is for int32
        $length = unpack("l", substr($span, $spanIdx, 4))[1];

        // V1: Byte array as real array version
        // NOTE: PHP's unpack() returns arrays on a 0-index basis
        $s = array_values(unpack('C*',substr($span, $spanIdx + 4, $length)));

        // // V2: Byte array as string version:
        // $s = substr($span, $spanIdx + 4, $length);

        $tdest[$destIdx++] = $s;

        $spanIdx = $spanIdx + 4 + $length;
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
  protected function readSingle(
    \jocoon\parquet\adapter\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    // length
    if($length === -1) $length = $reader->readInt32();

    // data
    // TODO: check whether this collides with the pack/unpack implementation above
    // NOTE: the BinaryReader interface returns those bytes as a string
    return $reader->readBytes($length);
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
    // V1: Byte array as real array version
    // NOTE: we're treating ByteArray Data as it is: array of bytes
    $writer->writeInt32(count($value));
    $writer->writeBytes($value); // or UTF8?

    // // V2: Byte array as string version:
    // // NOTE: we're treating ByteArray Data as it is: array of bytes
    // $writer->writeInt32(strlen($value));
    // $writer->writeString($value); // or UTF8?

  }

  /**
   * @inheritDoc
   */
  public function compare($x, $y): int
  {
    return 0;
  }

  /**
   * @inheritDoc
   */
  public function plainEncode(\jocoon\parquet\format\SchemaElement $tse, $x)
  {
    return $x;
  }

  /**
   * @inheritDoc
   */
  public function plainDecode(
    \jocoon\parquet\format\SchemaElement $tse,
    $encoded
  ) {
    return $encoded;
  }
}
