<?php
namespace jocoon\parquet\data\concrete;

use Exception;

use jocoon\parquet\ParquetException;

use jocoon\parquet\adapter\BinaryReader;
use jocoon\parquet\adapter\BinaryWriter;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\DecimalDataField;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;
use jocoon\parquet\format\SchemaElement;

use jocoon\parquet\values\primitives\BigDecimal;

class DecimalDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'string';
    parent::__construct(DataType::Decimal, Type::FIXED_LEN_BYTE_ARRAY, ConvertedType::DECIMAL);
  }

  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
  ): bool {

    return
      $tse->converted_type !== null && $tse->converted_type === ConvertedType::DECIMAL &&
      (
        $tse->type === Type::FIXED_LEN_BYTE_ARRAY ||
        $tse->type === Type::INT32 ||
        $tse->type === Type::INT64
      );
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

    //modify this element slightly
    $tse = end($container);

    if ($field instanceof DecimalDataField)
    {
      // NOTE: we're doing a little bit of switching between $dse and $tse here
      $dse = $field;
      if($dse->forceByteArrayEncoding)
      {
        $tse->type = Type::FIXED_LEN_BYTE_ARRAY;
      }
      else
      {
        if ($dse->precision <= 9)
          $tse->type = Type::INT32;
        else if ($dse->precision <= 18)
          $tse->type = Type::INT64;
        else
          $tse->type = Type::FIXED_LEN_BYTE_ARRAY;
      }

      $tse->precision = $dse->precision;
      $tse->scale = $dse->scale;
      $tse->type_length = BigDecimal::GetBufferSize($dse->precision);
    }
    else
    {
      //set defaults
      $tse->precision = 38;
      $tse->scale = 18;
      $tse->type_length = 16;
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
    $ddest = &$dest;

    switch ($tse->type)
    {
      case Type::INT32:
        return $this->ReadAsInt32($tse, $reader, $ddest, $offset);
      case Type::INT64:
        return $this->ReadAsInt64($tse, $reader, $ddest, $offset);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return $this->ReadAsFixedLengthByteArray($tse, $reader, $ddest, $offset);
      default:
      throw new Exception("data type '{$tse->type}' does not represent a decimal"); // TODO InvalidDataException
    }
  }

  /**
   * [ReadAsInt32 description]
   * @param  SchemaElement $tse    [description]
   * @param  BinaryReader  $reader [description]
   * @param  array         &$dest   [description]
   * @param  int           $offset [description]
   * @return int                   [description]
   */
  protected function ReadAsInt32(SchemaElement $tse, BinaryReader $reader, array &$dest, int $offset): int
  {
    $start = $offset;
    // decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
    $scaleFactor = bcpow(10, -$tse->scale, $tse->precision);

    while ($reader->getPosition() + 4 <= $reader->getEofPosition())
    {
      $iv = $reader->readInt32();
      // decimal dv = iv * scaleFactor;
      $dv = bcmul($iv, $scaleFactor, $tse->precision); // TODO: precision?
      $dest[$offset++] = $dv;
    }
    return $offset - $start;
  }

  /**
   * [WriteAsInt32 description]
   * @param SchemaElement $tse    [description]
   * @param BinaryWriter        $writer [description]
   * @param array         $values [description]
   */
  protected function WriteAsInt32(SchemaElement $tse, BinaryWriter $writer, array $values): void
  {
    $scaleFactor = bcpow(10, $tse->scale, $tse->precision); // Math.Pow(10, tse.Scale);
    foreach($values as $d) {
      try
      {
        $i = (int)bcmul($d, $scaleFactor, 0);
        $writer->writeInt32($i);
      }
      catch (Exception $e) // OverflowException ?
      {
        throw new ParquetException(
          "value '{$d}' is too large to fit into scale {$tse->scale} and precision {$tse->precision}"
        );
      }
    }
  }

  /**
   * [ReadAsInt64 description]
   * @param  SchemaElement $tse    [description]
   * @param  BinaryReader  $reader [description]
   * @param  array         &$dest   [description]
   * @param  int           $offset [description]
   * @return int                   [description]
   */
  protected function ReadAsInt64(SchemaElement $tse, BinaryReader $reader, array &$dest, int $offset): int
  {
    $start = $offset;
    // decimal scaleFactor = (decimal)Math.Pow(10, -tse.Scale);
    $scaleFactor = bcpow(10, -$tse->scale, $tse->precision);

    while ($reader->getPosition() + 8 <= $reader->getEofPosition())
    {
      $lv = $reader->readInt64();
      // decimal dv = lv * scaleFactor;
      $dv = bcmul($lv, $scaleFactor, $tse->precision); // TODO: precision?
      $dest[$offset++] = $dv;
    }
    return $offset - $start;
  }

  /**
   * [WriteAsInt64 description]
   * @param SchemaElement $tse    [description]
   * @param BinaryWriter        $writer [description]
   * @param array         $values [description]
   */
  protected function WriteAsInt64(SchemaElement $tse, BinaryWriter $writer, array $values): void
  {
    $scaleFactor = bcpow(10, $tse->scale, $tse->precision); // Math.Pow(10, tse.Scale);

    foreach ($values as $d)
    {
      try
      {
        // long l = (long)(d * (decimal)scaleFactor);
        $l = (int)bcmul($d, $scaleFactor, 0);
        $writer->writeInt64($l);
      }
      catch (Exception $e) // OverflowException
      {
        throw new ParquetException(
          "value '{$d}' is too large to fit into scale {$tse->scale} and precision {$tse->precision}"
        );
      }
    }
  }

  /**
   * [ReadAsFixedLengthByteArray description]
   * @param  SchemaElement $tse    [description]
   * @param  BinaryReader  $reader [description]
   * @param  array         &$dest   [description]
   * @param  int           $offset [description]
   * @return int                   [description]
   */
  protected function ReadAsFixedLengthByteArray(SchemaElement $tse, BinaryReader $reader, array &$dest, int $offset): int
  {
    $start = $offset;
    $typeLength = $tse->type_length;

    //can't read if there is no type length set
    if ($typeLength === 0) return 0;

    while (($reader->getPosition() + $typeLength) <= $reader->getEofPosition())
    {
      $itemData = $reader->readBytes($typeLength);
      // byte[] itemData = reader.ReadBytes(typeLength);
      // decimal dc = new BigDecimal(itemData, tse);
      $dc = BigDecimal::BinaryDataToDecimal($itemData, $tse);
      $dest[$offset++] = $dc;
    }

    return $offset - $start;
  }

  /**
   * [WriteAsFixedLengthByteArray description]
   * @param SchemaElement $tse    [description]
   * @param BinaryWriter        $writer [description]
   * @param array         $values [description]
   */
  protected function WriteAsFixedLengthByteArray(SchemaElement $tse, BinaryWriter $writer, array $values): void
  {
    foreach($values as $d)
    {
      // var bd = new BigDecimal(d, tse.Precision, tse.Scale);
      // byte[] itemData = bd.ToByteArray();

      $itemData = BigDecimal::DecimalToBinary($d, $tse->precision, $tse->scale);
      $tse->type_length = count($itemData); //always re-set type length as it can differ from default type length

      // writer.Write(itemData);
      $writer->writeBytes($itemData);
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
    switch ($tse->type)
    {
      case Type::INT32:
        $iscaleFactor = bcpow(10, -$tse->scale, $tse->precision); // (decimal)Math.Pow(10, -tse.Scale);
        $iv = $reader->readInt32();
        $idv = bcmul($iv, $iscaleFactor, $tse->precision); // TODO use bcmath?
        return $idv;
      case Type::INT64:
        $lscaleFactor = bcpow(10, -$tse->scale, $tse->precision); // (decimal)Math.Pow(10, -tse.Scale);
        $lv = $reader->ReadInt64();
        $ldv = bcmul($lv, $lscaleFactor, $tse->precision); // TODO use bcmath?
        return $ldv;
      case Type::FIXED_LEN_BYTE_ARRAY:
        $itemData = $reader->readBytes($tse->type_length);
        return BigDecimal::BinaryDataToDecimal($itemData, $tse);
        // echo("DECIMAL DEBUG = ");
        // print_r($r);
        // die();
        // return new BigDecimal(itemData, tse);
      default:
        throw new \Exception("data type '{$tse->type}' does not represent a decimal");
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
    switch($tse->type)
    {
      case Type::INT32:
        $this->WriteAsInt32($tse, $writer, $values);
        break;
      case Type::INT64:
        $this->WriteAsInt64($tse, $writer, $values);
        break;
      case Type::FIXED_LEN_BYTE_ARRAY:
        $this->WriteAsFixedLengthByteArray($tse, $writer, $values);
        break;
      default:
      throw new Exception("data type '{$tse->type}' does not represent a decimal"); // TODO InvalidDataException
    }
  }
}
