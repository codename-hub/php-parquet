<?php
namespace jocoon\parquet\data;

/**
 * [DataColumn description]
 */
class DataColumn
{
  /**
   * [public description]
   * @var int[]|null
   */
  public $repetitionLevels;

  /**
   * [protected description]
   * @var DataField
   */
  protected $field;

  /**
   * [protected description]
   * @var array
   */
  protected $data;

  /**
   * [protected description]
   * @var DataTypeHandlerInterface
   */
  protected $dataTypeHandler;

  /**
   * [public description]
   * @var DataColumnStatistics
   */
  public $statistics;

  /**
   * [__construct description]
   * @param DataField       $field            [description]
   * @param array           $data             [description]
   * @param int[]|null      $repetitionLevels [description]
   */
  public function __construct(DataField $field, array $data, ?array $repetitionLevels = null)
  {
    $this->init($field);

    $this->data = $data;
    $this->repetitionLevels = $repetitionLevels;

    $this->statistics = new DataColumnStatistics();
  }

  /**
   * [hasRepetitions description]
   * @return bool [description]
   */
  public function hasRepetitions(): bool {
    return $this->repetitionLevels !== null; // TODO: check for empty array?
  }

  /**
   * [getField description]
   * @return DataField [description]
   */
  public function getField() : DataField {
    return $this->field;
  }

  /**
   * [protected description]
   * @var bool[]
   */
  protected $hasValueFlags;

  /**
   * [DataColumnExtended description]
   * @param  DataField  $field              [description]
   * @param  array      $definedData        [description]
   * @param  array|null     $definitionLevels   [description]
   * @param  int        $maxDefinitionLevel [description]
   * @param  array|null     $repetitionLevels   [description]
   * @param  int        $maxRepetitionLevel [description]
   * @param  array      $dictionary         [description]
   * @param  array      $dictionaryIndexes  [description]
   * @return DataColumn                     [description]
   */
  public static function DataColumnExtended(
    DataField $field,
    array $definedData,
    ?array $definitionLevels, int $maxDefinitionLevel,
    ?array $repetitionLevels, int $maxRepetitionLevel,
    ?array $dictionary,
    ?array $dictionaryIndexes
  ) : DataColumn {
    $instance = new self($field, []);
    $instance->data = $definedData;

    // 1. Apply definitions
    if($definitionLevels !== null) {
      $hasValueFlags = [];
      $instance->data = $instance->dataTypeHandler->unpackDefinitions($instance->data, $definitionLevels, $maxDefinitionLevel, $hasValueFlags); // TODO!
      $instance->hasValueFlags = $hasValueFlags;
    }

    // 2. Apply repetitions
    $instance->repetitionLevels = $repetitionLevels;

    return $instance;
  }

  /**
   * [init description]
   * @param  DataField $field [description]
   * @return [type]           [description]
   */
  protected function init(DataField $field) {
    $this->field = $field;
    $this->dataTypeHandler = DataTypeFactory::matchType($field->dataType);
  }

  /**
   * [getData description]
   * @return array [description]
   */
  public function getData() : array {
    return $this->data;
  }

  /**
   * [CalculateRowCount description]
   * @return int [description]
   */
  public function CalculateRowCount(): int
  {
    if($this->field->maxRepetitionLevel > 0)
    {
      return count(array_filter($this->repetitionLevels, function($rl) { return $rl == 0; }));
    }

     return count($this->data);
  }

  /**
   * [PackDefinitions description]
   * @param  int   $maxDefinitionLevel     [description]
   * @param  int[] $pooledDefinitionLevels [description]
   * @param  int   $definitionLevelCount   [description]
   * @param  int   $nullCount              [description]
   * @return array                         [description]
   */
  public function PackDefinitions(int $maxDefinitionLevel, array &$pooledDefinitionLevels, int &$definitionLevelCount, int &$nullCount): array
  {
    $pooledDefinitionLevels = array_fill(0, count($this->data), 0); //  ArrayPool<int>.Shared.Rent(Data.Length);
    $definitionLevelCount = count($this->data);

    // bool isNullable = Field.ClrType.IsNullable() || Data.GetType().GetElementType().IsNullable();
    $isNullable = true; // ?

    if (!$this->field->hasNulls || !$isNullable)
    {
      $this->SetPooledDefinitionLevels($maxDefinitionLevel, $pooledDefinitionLevels);
      $nullCount = 0; //definitely no nulls here
      return $this->data;
    }

    // return _dataTypeHandler.PackDefinitions(Data, maxDefinitionLevel, out pooledDefinitionLevels, out definitionLevelCount, out nullCount);
    return $this->dataTypeHandler->PackDefinitions($this->data, $maxDefinitionLevel, $pooledDefinitionLevels, $definitionLevelCount, $nullCount);
  }

  /**
   * [SetPooledDefinitionLevels description]
   * @param int   $maxDefinitionLevel     [description]
   * @param int[] &$pooledDefinitionLevels [description]
   */
  function SetPooledDefinitionLevels(int $maxDefinitionLevel, array &$pooledDefinitionLevels): void
  {
    for ($i = 0; $i < count($this->data); $i++)
    {
      $pooledDefinitionLevels[$i] = $maxDefinitionLevel;
    }
  }

}
