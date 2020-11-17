<?php
namespace jocoon\parquet\data\concrete;

use DateTimeImmutable;

use jocoon\parquet\adapter\BinaryReader;
use jocoon\parquet\adapter\BinaryWriter;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\DateTimeFormat;
use jocoon\parquet\data\DateTimeDataField;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

use jocoon\parquet\helper\OtherExtensions;

use jocoon\parquet\values\primitives\NanoTime;

/**
 * [DateTimeOffsetDataTypeHandler description]
 */
class DateTimeOffsetDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'object';
    $this->phpClass = DateTimeImmutable::class;
    parent::__construct(DataType::DateTimeOffset, Type::INT96);
  }

  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
  ): bool {
    return
      ($tse->type === Type::INT96 && $formatOptions->TreatBigIntegersAsDates) || // Impala
      ($tse->type === Type::INT64 && isset($tse->converted_type) && $tse->converted_type === ConvertedType::TIMESTAMP_MILLIS) ||
      ($tse->type === Type::INT32 && isset($tse->converted_type) && $tse->converted_type === ConvertedType::DATE);
  }

  /**
   * @inheritDoc
   */
  public function createThrift(
    \jocoon\parquet\data\Field $field,
    \jocoon\parquet\format\SchemaElement $parent,
    array &$container
  ): void {
    parent::createThrift($field, $parent, $container);

    //modify annotations
    $tse = end($container);
    if($field instanceof DateTimeDataField) {
      $dse = $field;
      switch ($dse->dateTimeFormat)
      {
        case DateTimeFormat::DateAndTime:
          $tse->type = Type::INT64;
          $tse->converted_type = ConvertedType::TIMESTAMP_MILLIS;
          break;
        case DateTimeFormat::Date:
          $tse->type = Type::INT32;
          $tse->converted_type = ConvertedType::DATE;
          break;

        //other cases are just default
      }
    }
    else
    {
      //default annotation is fine
    }
  }

  /**
   * @inheritDoc
   */
  public function read(
    BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    array &$dest,
    int $offset
  ): int {
    switch($tse->type) {
      case Type::INT32:
        return $this->readAsInt32($reader, $dest, $offset);
      case Type::INT64:
        return $this->readAsInt64($reader, $dest, $offset);
      case Type::INT96:
        return $this->readAsInt96($reader, $dest, $offset);
      default:
        throw new \LogicException('Not supported type');
    }
  }

  /**
   * @inheritDoc
   */
  public function Write(
    \jocoon\parquet\format\SchemaElement $tse,
    \jocoon\parquet\adapter\BinaryWriter $writer,
    array $values,
    \jocoon\parquet\data\DataColumnStatistics $statistics = null
  ): void {

    switch($tse->type) {
      case Type::INT32:
        $this->WriteAsInt32($writer, $values);
        break;
      case Type::INT64:
        $this->WriteAsInt64($writer, $values);
        break;
      case Type::INT96:
        $this->WriteAsInt96($writer, $values);
        break;
      default:
       throw new \Exception("data type '{$tse->type}' does not represent any date types");
    }

    // calculate statistics if required
    if ($statistics !== null)
    {
      $this->calculateStatistics($values, $statistics);
    }
  }

  /**
   * [readAsInt32 description]
   * @param  BinaryReader $reader [description]
   * @param  array                       &$dest   [description]
   * @param  int                         $offset [description]
   * @return int
   */
  protected function readAsInt32(BinaryReader $reader, array &$dest, int $offset): int {
    $idx = $offset;

    while($reader->getPosition() + 4 <= $reader->getEofPosition()) {
      $iv = $reader->readInt32();
      $e = OtherExtensions::FromUnixDays($iv);
      $dest[$idx++] = $e;
    }

    return $idx - $offset;
  }

  /**
   * [WriteAsInt32 description]
   * @param \jocoon\parquet\adapter\BinaryWriter  $writer [description]
   * @param DateTimeImmutable[]   $values [description]
   */
  protected function WriteAsInt32(\jocoon\parquet\adapter\BinaryWriter $writer, array $values): void
  {
    foreach($values as $dto) {
      $days = OtherExtensions::ToUnixDays($dto);
      $writer->writeInt32($days);
    }
  }

  /**
   * [readAsInt64 description]
   * @param  BinaryReader $reader [description]
   * @param  array        &$dest   [description]
   * @param  int          $offset [description]
   * @return int
   */
  protected function readAsInt64(BinaryReader $reader, array &$dest, int $offset): int {
    $idx = $offset;

    while($reader->getPosition() + 8 <= $reader->getEofPosition()) {
      $lv = $reader->readInt64();
      $dto = OtherExtensions::FromUnixMilliseconds($lv);
      $dest[$idx++] = $dto;
    }

    return $idx - $offset;
  }

  /**
   * [WriteAsInt64 description]
   * @param \jocoon\parquet\adapter\BinaryWriter  $writer [description]
   * @param DateTimeImmutable[]   $values [description]
   */
  protected function WriteAsInt64(\jocoon\parquet\adapter\BinaryWriter $writer, array $values): void
  {
    foreach($values as $dto) {
      $value = OtherExtensions::ToUnixMilliseconds($dto);
      $writer->writeInt64($value);
    }
  }

  /**
   * [readAsInt96 description]
   * @param  BinaryReader $reader [description]
   * @param  array        &$dest   [description]
   * @param  int          $offset [description]
   * @return int
   */
  protected function readAsInt96(BinaryReader $reader, array &$dest, int $offset): int {
    $idx = $offset;

    while($reader->getPosition() + 8 <= $reader->getEofPosition()) {
      $bytes = $reader->readBytes(12);
      $dto = NanoTime::DateTimeImmutableFromNanoTimeBytes($bytes, 0);
      $dest[$idx++] = $dto;
    }

    return $idx - $offset;
  }

  /**
   * [WriteAsInt96 description]
   * @param \jocoon\parquet\adapter\BinaryWriter  $writer [description]
   * @param DateTimeImmutable[]   $values [description]
   */
  protected function WriteAsInt96(\jocoon\parquet\adapter\BinaryWriter $writer, array $values): void
  {
    foreach($values as $dto) {
      $nano = NanoTime::NanoTimeFromDateTimeImmutable($dto);
      $nano->write($writer);
    }
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    switch($tse->type) {
      case Type::INT32:
        $iv = $reader->readInt32();
        return OtherExtensions::FromUnixDays($iv);
      case Type::INT64:
        $lv = $reader->readInt64();
        return OtherExtensions::FromUnixMilliseconds($lv);
      case Type::INT96:
        $bytes = $reader->readBytes(12);
        return NanoTime::DateTimeImmutableFromNanoTimeBytes($bytes, 0);
      default:
        throw new \LogicException('Not supported type');
    }
  }

  /**
  * @inheritDoc
  */
  public function plainEncode(\jocoon\parquet\format\SchemaElement $tse, $x)
  {
    if($x === null) return null;

    $ms = fopen('php://memory', 'r+');
    $writer = BinaryWriter::createInstance($ms);
    $this->write($tse, $writer, [$x]);
    return $writer->toString();
  }

  /**
  * @inheritDoc
  */
  public function plainDecode(
    \jocoon\parquet\format\SchemaElement $tse,
    $encoded
  ) {
    if ($encoded === null) return null;
    $ms = fopen('php://memory', 'r+');
    fwrite($ms, $encoded);
    $reader = BinaryReader::createInstance($ms);
    return $this->readSingle($reader, $tse, -1);
  }
}
