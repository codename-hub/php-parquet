<?php
namespace codename\parquet\data;

/**
 * [Schema description]
 */
class Schema
{
  /**
   * [public description]
   * @var string
   */
  public const PathSeparator = ".";

  /**
   * [protected description]
   * @var Field[]
   */
  public $fields;

  /**
   * [__construct description]
   * @param Field[] $fields [description]
   */
  public function __construct(array $fields)
  {
    if(count($fields) === 0) {
      throw new \Exception('at least one field is required');
    }

    // TODO check fields / types for being instances of Field - or DataField?

    $this->fields = $fields;

    // set levels now, after schema is constructeds
    foreach($this->fields as $field)
    {
      $field->PropagateLevels(0, 0);
    }
  }

  /**
   * Returns (only) DataFields contained in the Schema
   * This __excludes__ non-DataFields (List, Struct, Map)
   * @return DataField[]
   */
  public function getDataFields() : array {
    $result = [];
    static::traverse($this->fields, $result);
    return $result;
  }


  protected static function traverse(array $fields, array &$result) {
    foreach($fields as $f) {
      static::analyse($f, $result);
    }
  }

  protected static function analyse(Field $f, array &$result) {
    switch ($f->schemaType) {
      case SchemaType::Data:
        $result[] = $f;
        break;
      case SchemaType::List:
        if($f instanceof ListField) {
          static::analyse($f->item, $result);
        } else {
          // error?
        }
        break;
      case SchemaType::Map:
        if($f instanceof MapField) {
          static::analyse($f->key, $result);
          static::analyse($f->value, $result);
        }
        break;
      case SchemaType::Struct:
        if($f instanceof StructField) {
          static::traverse($f->getFields(), $result);
        }
        break;
    }
  }

  /**
   * Compares this schema to another one and produces a human readable message describing the differences.
   * @param  Schema $other                   [description]
   * @param  string $thisName                [description]
   * @param  string $otherName               [description]
   * @return string            [description]
   */
  public function GetNotEqualsMessage(Schema $other, string $thisName, string $otherName): string {
    if(count($this->fields) !== count($other->fields)) {
      return 'different number of elements ('.count($this->fields).' != '.count($other->fields).')';
    }

    $parts = [];
    foreach($this->fields as $i => $field) {
      if(!$field->Equals($other->fields[$i])) {
        $parts[] = "[{$thisName}: {$field->name}] != [{$other->fields[$i]->name}]";
      }
    }
    if(count($parts) > 0) {
      return implode(', ', $parts);
    }
    return 'not sure!';
  }

  /**
   * [Equals description]
   * @param  [type] $other [description]
   * @return bool          [description]
   */
  public function Equals($other): bool
  {
    if ($other === null) return false;
    if ($other === $this) return true;

    if($other instanceof Schema) {

      if(count($this->fields) !== count($other->fields)) return false;

      foreach($this->fields as $i => $field) {
        if(!$field->Equals($other->fields[$i])) return false;
      }

      return true;

    } else {
      // $other is not an instance of Schema
      return false;
    }
  }

}
