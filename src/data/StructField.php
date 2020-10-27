<?php
namespace jocoon\parquet\data;

use Exception;

use jocoon\parquet\helper\OtherExtensions;

class StructField extends Field
{
  /**
   * [protected description]
   * @var Field[]
   */
  protected $fields = [];

  /**
   * @inheritDoc
   */
  protected function __construct(string $name, ?array $elements = null)
  {
    parent::__construct($name, SchemaType::Struct);

    if($elements && count($elements) === 0) {
      // throw new ArgumentException($"structure '{name}' requires at least one element");
      throw new Exception("structure '{name}' requires at least one element");
    }

     //path for structures has no weirdnes, yay!
     if($elements) {
       foreach($elements as $field)
       {
         $this->fields[] = $field;
       }
     }

     $this->path = $name;
     $this->setPathPrefix(null);
  }

  /**
   * [getFields description]
   * @return Field[] [description]
   */
  public function getFields() : array {
    return $this->fields;
  }

  /**
   * [createWithField description]
   * @param  string      $name  [description]
   * @param  Field       $field [description]
   * @return StructField        [description]
   */
  public static function createWithField(string $name, Field $field): StructField {
    return new StructField($name, [ $field ]);
  }

  /**
   * [createWithFieldArray description]
   * @param  string       $name     [description]
   * @param  Field[]      $elements [description]
   * @return StructField            [description]
   */
  public static function createWithFieldArray(string $name, array $elements): StructField {
    return new StructField($name, $elements);
  }

  /**
   * @inheritDoc
   */
  public function setPathPrefix($value)
  {
    $this->path = OtherExtensions::AddPath($value, $this->name);

    foreach($this->fields as $field) {
      $field->setPathPrefix($this->path);
    }
  }

  /**
   * @inheritDoc
   */
  public function assign(\jocoon\parquet\data\Field $field): void
  {
    $this->fields[] = $field;
  }

  /**
   * @inheritDoc
   */
  public function PropagateLevels(
    int $parentRepetitionLevel,
    int $parentDefinitionLevel
  ): void {
    //struct is a container, it doesn't have any levels

    foreach($this->fields as $f)
    {
      $f->PropagateLevels($parentRepetitionLevel, $parentDefinitionLevel);
    }
  }

  /**
   * [CreateWithNoElements description]
   * @param  string      $name [description]
   * @return StructField       [description]
   */
  public static function CreateWithNoElements(string $name): StructField {
    return new StructField($name);
  }

  /**
   * @inheritDoc
   */
  public function Equals($other): bool
  {
    if($other === null) return false;
    if($other === $this) return true;
    if (get_class($other) != get_class($this)) return false;

    if($other instanceof StructField) {

      if($this->name != $other->name) return false;
      if(count($this->fields) !== count($other->fields)) return false;

      foreach($this->fields as $i => $field) {
        if(!$field->Equals($other->fields[$i])) return false;
      }

      return true;

    } else {
      throw new Exception('Invalid equality comparison');
    }
  }
}
