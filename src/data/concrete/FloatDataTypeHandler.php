<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

class FloatDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'double'; // Yeah, it is: https://www.php.net/manual/en/function.gettype.php
    parent::__construct(DataType::Float, Type::FLOAT);
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \PhpBinaryReader\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) : float {
    //
    // Oh really. We have to enforce a pseudo-rounding
    // As PHP stores doubles internally and we get floating point fluctuation
    // almost every time. Now, this should work, as we're cutting off
    // decimal places after the 7th (the 'real' float, if you know what I mean. Pun intended)
    //
    return round($reader->readSingle(), 7);

    // return $reader->readSingle();
    // $v = (float)$reader->readSingle();
    // echo("float - ReadSingle: ");
    // var_dump([$v, floatval($v)]);
    // return $v;
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\Nelexa\Buffer\Buffer $writer, $value): void
  {
    $writer->insertFloat($value);
  }
}
