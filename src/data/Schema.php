<?php
namespace jocoon\parquet\data;

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
   * [getDataFields description]
   * @return array [description]
   */
  public function getDataFields() : array {
    $result = [];

    // $analyse = function (Field $f) use (&$result) {
    //   switch ($f->schemaType) {
    //     case SchemaType::Data:
    //       $result[] = $f;
    //       // result.Add((DataField)f);
    //       break;
    //     case SchemaType::List:
    //       // if($f instanceof ListField) {
    //       //   $analyse($f->item);
    //       // }
    //       // $analyse(((ListField)$f));
    //       break;
    //     case SchemaType::Map:
    //       if($f instanceof MapField) {
    //         $analyse($f->key);
    //         $analyse($f->value);
    //       }
    //       // MapField mf = (MapField)f;
    //       // analyse(mf.Key);
    //       // analyse(mf.Value);
    //       break;
    //     case SchemaType::Struct:
    //       if($f instanceof StructField) {
    //         $traverse($f->fields);
    //       }
    //       // StructField sf = (StructField)f;
    //       // traverse(sf.Fields);
    //       break;
    //   }
    // };

    // $traverse = function(array $fields) use ($analyse) {
    //   foreach($fields as $f) {
    //     $analyse(
    //       $f
    //     );
    //   }
    // };

    // $traverse($this->fields);

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
        // result.Add((DataField)f);
        break;
      case SchemaType::List:
        if($f instanceof ListField) {
          static::analyse($f->item, $result);
        } else {
          // error?
        }
        break;
        // static::analyse($f);
        // if($f instanceof ListField) {
        //   $analyse($f->item);
        // }
        // $analyse(((ListField)$f));
        break;
      case SchemaType::Map:
        if($f instanceof MapField) {
          static::analyse($f->key, $result);
          static::analyse($f->value, $result);
        }
        // MapField mf = (MapField)f;
        // analyse(mf.Key);
        // analyse(mf.Value);
        break;
      case SchemaType::Struct:
        if($f instanceof StructField) {
          static::traverse($f->getFields(), $result);
        }
        // StructField sf = (StructField)f;
        // traverse(sf.Fields);
        break;
    }
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

    // for(int i = 0; i < _fields.Count; i++)
    // {
    //   if (!_fields[i].Equals(other._fields[i])) return false;
    // }
  }

}
