<?php
namespace codename\parquet\data;

use Exception;

use codename\parquet\helper\OtherExtensions;

class MapField extends Field
{
  /**
   * [ContainerName description]
   * @var string
   */
  const ContainerName = 'key_value';

  /**
   * [public description]
   * @var Field
   */
  public $key;

  /**
   * [public description]
   * @var Field
   */
  public $value;

  /**
   * Whether the map field itself is nullable
   * @var bool
   */
  public $hasNulls;

  // /**
  //  * Whether the map field itself is repeatable
  //  * @var bool
  //  */
  // public $isArray;

  /**
   * Whether the map values are nullable
   * @var bool
   */
  public $keyValueHasNulls;

  /**
   * @inheritDoc
   */
  public function __construct(
    string $name,
    ?DataField $keyField = null,
    ?Field $valueField = null,
    bool $nullable = false,
    bool $keyValueNullable = true // TODO: remove
  ) {
    parent::__construct($name, SchemaType::Map);

    $this->hasNulls = $nullable;
    $this->keyValueHasNulls = $keyValueNullable;

    if($keyField) {
      // key is required in maps, therefore: make not-nullable internally
      // just to make sure
      $keyField->hasNulls = false;
    }

    // keyField and valueField are optional
    // in dotnet, this is a separate constructor.
    if($keyField && $valueField) {
      $this->key = $keyField;
      $this->value = $valueField;
      $this->setPath(OtherExtensions::AddPath($name, static::ContainerName));
      $this->key->setPathPrefix($this->path);
      $this->value->setPathPrefix($this->path);
    }
  }

  /**
   * @inheritDoc
   */
  public function setPathPrefix($value)
  {
    $this->setPath(OtherExtensions::AddPath($value, [ $this->name, static::ContainerName ]));
    $this->key->setPathPrefix($this->path);
    $this->value->setPathPrefix($this->path);
  }

  /**
   * @inheritDoc
   */
  public function PropagateLevels(
    int $parentRepetitionLevel,
    int $parentDefinitionLevel
  ): void {

    $rl = $parentRepetitionLevel;
    $dl = $parentDefinitionLevel;

    //"container" is optional and adds on 1 DL
    $dl += 1;

    //"key_value" is repeated therefore it adds on 1 RL + 1 DL
    $rl += 1;
    $dl += 1;

    //push to children
    $this->key->PropagateLevels($rl, $dl);
    $this->value->PropagateLevels($rl, $dl);
  }

  /**
   * @inheritDoc
   */
  public function assign(\codename\parquet\data\Field $field): void
  {
    if($this->key === null) {
      $this->key = $field;
    } else if($this->value === null) {
      $this->value = $field;
    } else {
      // throw new InvalidOperationException($"'{Name}' already has key and value assigned");
      throw new Exception("'{Name}' already has key and value assigned");
    }
  }
}
