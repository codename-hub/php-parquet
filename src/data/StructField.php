<?php
namespace codename\parquet\data;

use Exception;

use codename\parquet\helper\OtherExtensions;

class StructField extends Field
{
  /**
   * [protected description]
   * @var Field[]
   */
  protected $fields = [];

  /**
   * [public description]
   * @var bool
   */
  public $hasNulls;

  /**
   * [public description]
   * @var bool
   */
  public $isArray;

  /**
   * @inheritDoc
   */
  protected function __construct(string $name, ?array $elements, bool $nullable, bool $isArray)
  {
    parent::__construct($name, SchemaType::Struct);

    $this->hasNulls = $nullable;
    $this->isArray = $isArray;

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

     $this->setPath([ $name ]);
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
   * @param  bool        $nullable
   * @param  bool        $isArray
   * @return StructField        [description]
   */
  public static function createWithField(string $name, Field $field, bool $nullable = false, bool $isArray = false): StructField {
    return new StructField($name, [ $field ], $nullable, $isArray);
  }

  /**
   * [createWithFieldArray description]
   * @param  string       $name     [description]
   * @param  Field[]      $elements [description]
   * @param  bool        $nullable
   * @param  bool        $isArray
   * @return StructField            [description]
   */
  public static function createWithFieldArray(string $name, array $elements, bool $nullable = false, bool $isArray = false): StructField {
    return new StructField($name, $elements, $nullable, $isArray);
  }

  /**
   * @inheritDoc
   */
  public function setPathPrefix($value)
  {
    $this->setPath(OtherExtensions::AddPath($value, $this->name));
    foreach($this->fields as $field) {
      $field->setPathPrefix($this->path);
    }
  }

  /**
   * @inheritDoc
   */
  public function assign(\codename\parquet\data\Field $field): void
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
    // struct is a container, it doesn't have any levels
    // NOTE: but it can be nullable, not-nullable or repeated

    // Structs might be repeated or optional
    // (nullable or even an array)
    //
    $dl = $parentDefinitionLevel;
    $rl = $parentRepetitionLevel;

    if($this->hasNulls) {
      $dl++;
    }
    if($this->isArray) {
      $rl++;
    }

    foreach($this->fields as $f)
    {
      $f->PropagateLevels($rl, $dl);
    }
  }

  /**
   * [CreateWithNoElements description]
   * @param  string      $name [description]
   * @param  bool        $nullable
   * @param  bool        $isArray
   * @return StructField       [description]
   */
  public static function CreateWithNoElements(string $name, bool $nullable = false, bool $isArray = false): StructField {
    return new StructField($name, null, $nullable, $isArray);
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
