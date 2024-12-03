<?php
namespace codename\parquet\data\concrete;

use codename\parquet\data\DataType;
use codename\parquet\data\BasicDataTypeHandler;
use codename\parquet\data\BasicPrimitiveDataTypeHandler;

use codename\parquet\format\Type;
use codename\parquet\format\ConvertedType;

class BooleanDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'boolean';
    parent::__construct(DataType::Boolean, Type::BOOLEAN);
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \codename\parquet\adapter\BinaryReader $reader,
    \codename\parquet\format\SchemaElement $tse,
    int $length
  ) : bool {
    return $reader->readBytes(1) !== null; // ??
    // return boolval($reader->readBytes(1)); //dunno if this is right...
    // $retval = boolval($raw = $reader->readBytes(1)); // dunno if this is right...
    // var_dump(['retval' => $retval, 'raw' => $raw]);
    // return $retval;
  }

  /**
   * @inheritDoc
   */
  public function read(
    \codename\parquet\adapter\BinaryReader $reader,
    \codename\parquet\format\SchemaElement $tse,
    array &$dest,
    int $offset
  ): int {
    $start = $offset;

    $ibit = 0;
    $bdest = &$dest;

    while ($reader->getPosition() < $reader->getEofPosition() && $offset < count($dest))
    {
      // NOTE: "byte" shifting for PHP...
      $b = ord($reader->readBytes(1)); // .ReadByte();

      while ($ibit < 8 && $offset < count($dest))
      {
        $set = (($b >> $ibit++) & 1) == 1;
        $bdest[$offset++] = $set;
      }

      $ibit = 0;
    }


    return $offset - $start;
  }

  /**
   * @inheritDoc
   */
  public function Write(
    \codename\parquet\format\SchemaElement $tse,
    \codename\parquet\adapter\BinaryWriter $writer,
    array $values,
    ?\codename\parquet\data\DataColumnStatistics $statistics = null
  ): void {
    $n = 0;
    $b = 0; // byte
    $buffer = array_fill(0, (int)(count($values) / 8 + 1), null);
    $ib = 0;

    foreach ($values as $flag)
    {
      if ($flag)
      {
        $b |= (1 << $n);
      }

      $n++;
      if ($n == 8)
      {
        $buffer[$ib++] = $b;
        $n = 0;
        $b = 0;
      }
    }

    if ($n !== 0) $buffer[$ib] = $b;

    $writer->writeBytes($buffer);
  }

  /**
   * @inheritDoc
   */
  public function plainEncode(\codename\parquet\format\SchemaElement $tse, $x)
  {
    return null;
  }

  /**
   * @inheritDoc
   */
  public function plainDecode(
    \codename\parquet\format\SchemaElement $tse,
    $encoded
  ) {
    return null;
  }
}
