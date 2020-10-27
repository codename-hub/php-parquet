<?php
namespace jocoon\parquet\data;

use Exception;

use jocoon\parquet\helper\OtherExtensions;

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
   * @inheritDoc
   */
  public function __construct(string $name, ?DataField $keyField = null, ?DataField $valueField = null)
  {
    parent::__construct($name, SchemaType::Map);

    // keyField and valueField are optional
    // in dotnet, this is a separate constructor.
    if($keyField && $valueField) {
      $this->key = $keyField;
      $this->value = $valueField;
      $this->path = OtherExtensions::AddPath($name, static::ContainerName); // TODO: Check path separator in this case...
      $this->key->setPathPrefix($this->path);
      $this->value->setPathPrefix($this->path);
    }
  }

  /**
   * @inheritDoc
   */
  public function setPathPrefix($value)
  {
    $this->path = OtherExtensions::AddPath($value, [ $this->name, static::ContainerName ]);
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
  public function assign(\jocoon\parquet\data\Field $field): void
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
