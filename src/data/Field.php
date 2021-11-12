<?php
namespace codename\parquet\data;

use Exception;

use codename\parquet\format\SchemaElement;

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
   * Path to field and/or field data
   * please use setPath() for setting
   * @var string[]
   */
  public $path;

  /**
   * Path as string - do not set
   * use ->setPath for setting
   * @var string
   */
  public $pathString;

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
   * @var string[]|null
   */
  protected $pathPrefix = null;

  /**
   * [setPathPrefix description]
   * @param string[]|null $value [description]
   */
  public function setPathPrefix(?array $value) {
    // used internally
  }

  /**
   * sets the path on the field
   * @param string[]|null   $path  [description]
   */
  public function setPath(?array $path): void {
    $this->path = $path;
    // helper variable, path as string
    $this->pathString = $this->path === null ? null : implode(\codename\parquet\data\Schema::PathSeparator, $this->path);
  }

  /**
   * @param string     $name
   * @param SchemaType $schemaType
   */
  protected function __construct(string $name, int $schemaType)
  {
    $this->name = $name;
    $this->schemaType = $schemaType;
    $this->setPath([ $name ]);
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
