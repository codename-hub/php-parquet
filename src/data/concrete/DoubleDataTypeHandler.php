<?php
namespace jocoon\parquet\data\concrete;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\BasicPrimitiveDataTypeHandler;

use jocoon\parquet\format\Type;
use jocoon\parquet\format\ConvertedType;

class DoubleDataTypeHandler extends BasicPrimitiveDataTypeHandler
{
  /**
   */
  public function __construct()
  {
    $this->phpType = 'double';
    parent::__construct(DataType::Double, Type::DOUBLE);
  }

  /**
   * @inheritDoc
   */
  protected function readSingle(
    \PhpBinaryReader\BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) : float {
    // throw new \Exception('Double not implemented yet due to PHP clarification needed');
    // return $reader->readDouble();

    $bytes = $reader->readBytes(8);
    $b = \unpack('d', $bytes);
    // var_dump($b);
    return $b[1];
  }

  /**
   * @inheritDoc
   */
  protected function WriteOne(\Nelexa\Buffer\Buffer $writer, $value): void
  {
    $writer->insertDouble($value); // re-pack?
  }
}
