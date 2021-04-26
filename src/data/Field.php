<?php
namespace jocoon\parquet\data;

use Exception;

use jocoon\parquet\format\SchemaElement;

/**
 * [abstract description]
 */
abstract class Field
{
  /**
   * [public description]
   * @var SchemaType
   */
  public $schemaType;

  /**
   * [public description]
   * @var string
   */
  public $name;

  /**
   * [public description]
   * @var string
   */
  public $path;

  /**
   * [public description]
   * @var int
   */
  public $maxRepetitionLevel = 0;

  /**
   * [public description]
   * @var int
   */
  public $maxDefinitionLevel = 0;

  /**
   * [public description]
   * @var string|null
   */
  protected $pathPrefix = null;

  /**
   * [setPathPrefix description]
   * @param [type] $value [description]
   */
  public function setPathPrefix($value) {
    // used internally
  }


  /**
   * @param string     $name
   * @param SchemaType $schemaType
   */
  protected function __construct(string $name, int $schemaType)
  {
    $this->name = $name;
    $this->schemaType = $schemaType;
    $this->path = $name;
  }

  /**
   * [PropagateLevels description]
   * @param int $parentRepetitionLevel [description]
   * @param int $parentDefinitionLevel [description]
   */
  public abstract function PropagateLevels(int $parentRepetitionLevel, int $parentDefinitionLevel) : void;

  /**
   * [assign description]
   * @param Field $field [description]
   */
  public function assign(Field $field): void {
    //only used by some schema fields internally to help construct a field hierarchy
  }

  /**
   * [Equals description]
   * @param  [type] $tse [description]
   * @return bool        [description]
   */
  public function Equals($tse): bool
  {
    if ($tse === null) return false;

    if($tse instanceof SchemaElement) {
      return $tse->name == $this->name;
    } else {
      return false;
    }
  }

}
