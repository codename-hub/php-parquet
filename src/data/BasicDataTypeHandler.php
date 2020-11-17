<?php
namespace jocoon\parquet\data;

use Exception;

use jocoon\parquet\adapter\BinaryReader;
use jocoon\parquet\adapter\BinaryWriter;

use jocoon\parquet\format\Statistics;
use jocoon\parquet\format\SchemaElement;
use jocoon\parquet\format\FieldRepetitionType;

abstract class BasicDataTypeHandler implements DataTypeHandlerInterface
{
  /**
   * [private description]
   * @var [type]
   */
  private $thriftType;

  private $convertedType;

  protected $dataType;

  public $phpType;

  public $phpClass;

  /**
   * @param mixed  $dataType
   * @param [type] $thriftType
   * @param mixed  $convertedType
   */
  public function __construct($dataType, $thriftType, $convertedType = null)
  {
    $this->dataType = $dataType;
    $this->thriftType = $thriftType;
    $this->convertedType = $convertedType;
  }

  /**
   * @inheritDoc
   */
  public function getSchemaType(): int
  {
    throw new \LogicException('Not implemented'); // TODO
  }

  /**
   * @inheritDoc
   */
  public function getDataType(): int
  {
    return $this->dataType;
  }

  /**
   * @inheritDoc
   */
  public function mergeDictionary(
    array $dictionary,
    array $indexes,
    array &$data,
    int $offset,
    int $length
  ): array {

    $result = &$data;

    // Performance HACK, avoid consecutive calls to count() of the dictionary arg
    // which doesnt change during method execution.
    $dictionaryCount = \count($dictionary);

    for ($i = 0; $i < $length; $i++)
    {
      $index = $indexes[$i];
      if ($index < $dictionaryCount)
      {
        // may not be true when value is null
        $value = $dictionary[$index];
        $result[$offset + $i] = $value;
      }
    }

    return $result;
  }

  /**
   * @inheritDoc
   */
  public function createSchemaElement(
    array $schema,
    int &$index,
    int &$ownedChildCount
  ) : Field {
    $tse = $schema[$index++];
    if($tse instanceof SchemaElement) {
      $hasNulls = $tse->repetition_type !== FieldRepetitionType::REQUIRED;
      $isArray = $tse->repetition_type === FieldRepetitionType::REPEATED;

      $simple = $this->createSimple($tse, $hasNulls, $isArray);
      $ownedChildCount = 0;
      return $simple;
    } else {
      throw new \Exception('BAD!');
    }
  }

  /**
   * [createSimple description]
   * @param  SchemaElement $tse      [description]
   * @param  bool          $hasNulls [description]
   * @param  bool          $isArray  [description]
   * @return DataField               [description]
   */
  protected function createSimple(SchemaElement $tse, bool $hasNulls, bool $isArray) : DataField {
    return new DataField($tse->name, $this->getDataType(), $hasNulls, $isArray);
  }

  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
  ): bool {
    // NOTE: php evaluates a zero (0) to false, therefore we have to check for !== null instead of being truthy, somehow. (type == 0 => boolean)
    return ($tse->type !== null && $this->thriftType === $tse->type) &&
      ($this->convertedType === null || $tse->converted_type && $tse->converted_type === $this->convertedType);
  }
  //
  // public function read(\jocoon\parquet\file\ThriftStream $stream) {
  //   $stream->Read()
  // }

  // /**
  //  * @inheritDoc
  //  */
  // public function getArray(int $minCount, bool $rent, bool $isNullable): array
  // {
  //   throw new \LogicException('Not implemented'); // TODO
  // }
  //
  /**
   * @inheritDoc
   */
  public function read(
    BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    array &$dest,
    int $offset
  ): int {
      return $this->readInternal($tse, $reader, $dest, $offset);
  }

  /**
   * @inheritDoc
   */
  public function readObject(
    BinaryReader $reader,
    \jocoon\parquet\format\SchemaElement $tse,
    int $length
  ) {
    return $this->readSingle($reader, $tse, $length);
  }

  protected abstract function readSingle(BinaryReader $reader, SchemaElement $tse, int $length);

  private function readInternal(SchemaElement $tse, BinaryReader $reader, array &$dest, int $offset) : int {
    $totalLength = $reader->getEofPosition();
    $idx = $offset;

    $destCount = \count($dest);

    while ($reader->getPosition() < $totalLength && $idx < $destCount)
    {
      $element = $this->readSingle($reader, $tse, -1);  //potential performance hit on calling a method
      $dest[$idx++] = $element;
    }

    return $idx - $offset;
  }

  protected function unpackGenericDefinitions(array $src, array $definitionLevels, int $maxDefinitionLevel, array &$hasValueFlags) : array
  {
    $result = array_fill(0, count($definitionLevels), null); // (T[])GetArray(definitionLevels.Length, false, true);
    $hasValueFlags = array_fill(0, count($definitionLevels), false); // new bool[definitionLevels.Length];

    $isrc = 0;
    for ($i = 0; $i < \count($definitionLevels); $i++)
    {
      $level = $definitionLevels[$i];

      if ($level == $maxDefinitionLevel)
      {
        $result[$i] = $src[$isrc++];
      }
    }

    return $result;
  }

  /**
   * @inheritDoc
   */
  public function packDefinitions(
    array $data,
    int $maxDefinitionLevel,
    array &$definitions,
    int &$definitionsLength,
    int &$nullCount
  ): array {
    return $this->packDefinitionsNullable($data, $maxDefinitionLevel, $definitions, $definitionsLength, $nullCount);
  }

  /**
   * @inheritDoc
   */
  public function packDefinitionsNullable(
    array $data,
    int $maxDefinitionLevel,
    array &$definitionLevels,
    int &$definitionsLength,
    int &$nullCount
  ): array {
    // Micro-Optimization
    $dataCount = \count($data);

    $definitionLevels = \array_fill(0, $dataCount, 0); // QUESTION/TODO: fill with null or 0?
    $definitionsLength = $dataCount;

    // $nullCount = count(array_filter($data, function($i) { return $i === null; }));
    $nullCount = 0;
    foreach($data as $i) {
      if($i === null) {
        $nullCount++;
      }
    }
    $result = \array_fill(0, $dataCount - $nullCount, null);

    $ir = 0;
    for ($i = 0; $i < $dataCount; $i++)
    {
      $value = $data[$i];

      if ($value === null)
      {
        $definitionLevels[$i] = 0;
      }
      else
      {
        $definitionLevels[$i] = $maxDefinitionLevel;
        $result[$ir++] = $value;
      }
    }

    return $result;
  }


  /**
   * [CreateThrift description]
   * @param Field               $se        [description]
   * @param SchemaElement       $parent    [description]
   * @param SchemaElement[]     &$container [description]
   */
  public function CreateThrift(Field $se, SchemaElement $parent, array &$container): void
  {
    if(($sef = $se) instanceof DataField) {
      $tse = new SchemaElement(['name' => $se->name]);
      $tse->type = $this->thriftType;
      if ($this->convertedType !== null) $tse->converted_type = $this->convertedType; // .Value;

      $tse->repetition_type = $sef->isArray
        ? FieldRepetitionType::REPEATED
        : ($sef->hasNulls ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED);

      $container[] = $tse;
      $parent->num_children += 1;
    } else {
      // TODO: throw exception, not a DataField ?
    }
  }

  /**
   * [Write description]
   * @param SchemaElement $tse        [description]
   * @param BinaryWriter        $writer     [description]
   * @param array         $values     [description]
   * @param DataColumnStatistics    $statistics [description]
   */
  public function Write(SchemaElement $tse, BinaryWriter $writer, array $values, DataColumnStatistics $statistics = null): void
  {
    foreach($values as $one) {
      $this->WriteOne($writer, $one);
    }

    // calculate statistics if required
    if ($statistics !== null)
    {
      $this->calculateStatistics($values, $statistics);
    }
  }

  /**
   * [calculateStatistics description]
   * @param array                $values     [description]
   * @param DataColumnStatistics $statistics [description]
   */
  protected function calculateStatistics(array $values, DataColumnStatistics $statistics): void {

    // number of distinct values
    $statistics->distinctCount = count(array_unique($values, SORT_REGULAR)); // ((TSystemType[])values).Distinct(this).Count();

    $min = null; // default value for type?
    $max = null; // default value for type?

    foreach($values as $i => $current) {

      if($i === 0) {
        $min = $current;
        $max = $current;
      } else {
        $cmin = $this->compare($min, $current);
        $cmax = $this->compare($max, $current);

        if($cmin > 0) {
          $min = $current;
        }
        if($cmax < 0) {
          $max = $current;
        }
      }
    }

    $statistics->minValue = $min;
    $statistics->maxValue = $max;
  }

  /**
   * [Compare description]
   * @param  [type] $x [description]
   * @param  [type] $y [description]
   * @return int       [description]
   */
  public abstract function compare($x, $y): int;

  /**
   * [WriteOne description]
   * @param BinaryWriter $writer [description]
   * @param [type] $value  [description]
   */
  protected function WriteOne(BinaryWriter $writer, $value): void
  {
    throw new Exception('Not supported');
  }
}
