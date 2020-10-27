<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

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
  protected function readSingle(
    \PhpBinaryReader\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    switch ($tse->type)
    {
      case Type::INT32:
        $iscaleFactor = pow(10, -$tse->scale); // (decimal)Math.Pow(10, -tse.Scale);
        $iv = $reader->readInt32();
        $idv = $iv * $iscaleFactor; // TODO use bcmath?
        return $idv;
      case Type::INT64:
        $lscaleFactor = pow(10, -$tse->scale); // (decimal)Math.Pow(10, -tse.Scale);
        $lv = $reader->ReadInt64();
        $ldv = $lv * $lscaleFactor; // TODO use bcmath?
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
}
