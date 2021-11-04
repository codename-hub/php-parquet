<?php
namespace codename\parquet\data;

use codename\parquet\ParquetOptions;
;
use codename\parquet\data\concrete\MapDataTypeHandler;
use codename\parquet\data\concrete\ByteDataTypeHandler;
use codename\parquet\data\concrete\ListDataTypeHandler;
use codename\parquet\data\concrete\FloatDataTypeHandler;
use codename\parquet\data\concrete\DoubleDataTypeHandler;
use codename\parquet\data\concrete\Int16DataTypeHandler;
use codename\parquet\data\concrete\Int32DataTypeHandler;
use codename\parquet\data\concrete\Int64DataTypeHandler;

use codename\parquet\data\concrete\UnsignedInt32DataTypeHandler;
use codename\parquet\data\concrete\UnsignedInt16DataTypeHandler;

use codename\parquet\data\concrete\ShortDataTypeHandler;
use codename\parquet\data\concrete\UnsignedInt64DataTypeHandler;
use codename\parquet\data\concrete\UnsignedShortDataTypeHandler;

use codename\parquet\data\concrete\StringDataTypeHandler;
use codename\parquet\data\concrete\BooleanDataTypeHandler;
use codename\parquet\data\concrete\DecimalDataTypeHandler;
use codename\parquet\data\concrete\ByteArrayDataTypeHandler;
use codename\parquet\data\concrete\StructureDataTypeHandler;
use codename\parquet\data\concrete\DateTimeOffsetDataTypeHandler;

use codename\parquet\exception\NotSupportedException;

use codename\parquet\format\SchemaElement;

class DataTypeFactory
{
  /**
   * [private description]
   * @var DataTypeHandlerInterface[]
   */
  private static $allDataTypes = null;

  /**
   * static initializer function for creating
   * all the generic data type handler instances
   * @return void
   */
  public static function initialize() {
    static::$allDataTypes = static::$allDataTypes ?? [
      // special types
      new DateTimeOffsetDataTypeHandler(),
      // new DateTimeDataTypeHandler(),
      // new IntervalDataTypeHandler(),
      new DecimalDataTypeHandler(),

      // low priority types
      new BooleanDataTypeHandler(),
      new ByteDataTypeHandler(),
      // new UnsignedByteDataTypeHandler(), // TODO
      // new SignedByteDataTypeHandler(),

      new ShortDataTypeHandler(),
      new UnsignedShortDataTypeHandler(),
      new Int16DataTypeHandler(),
      new UnsignedInt16DataTypeHandler(),
      new UnsignedInt32DataTypeHandler(),

      new Int32DataTypeHandler(),

      new UnsignedInt64DataTypeHandler(),
      new Int64DataTypeHandler(),
      // new Int96DataTypeHandler(),


      new FloatDataTypeHandler(),
      new DoubleDataTypeHandler(),
      new StringDataTypeHandler(),
      new ByteArrayDataTypeHandler(),

      // composite types
      new ListDataTypeHandler(),
      new MapDataTypeHandler(),
      new StructureDataTypeHandler()
    ];
  }

  // public static function match() {
  //   $args = func_get_args();
  //   if($args[0] instanceof SchemaElement) {
  //     return static::matchSchemaElement($args[0], $args[1]);
  //   }
  // }

  /**
   * [matchSchemaElement description]
   * @param  SchemaElement            $tse            [description]
   * @param  ParquetOptions|null      $formatOptions  [description]
   * @return DataTypeHandlerInterface|null                 [description]
   */
  public static function matchSchemaElement(SchemaElement $tse, ?ParquetOptions $formatOptions) : ?DataTypeHandlerInterface {
    foreach(static::$allDataTypes as $dt) {


      if($dt->isMatch($tse, $formatOptions)) {
        // echo("MATCH!");
        // print_r($tse);
        // print_r($dt);
        return $dt;
      }
    }

    // echo("no Match");
    // print_r($tse);
    return null;
  }

  /**
   * [match description]
   * @param  Field  $field [description]
   * @return DataTypeHandlerInterface
   */
  public static function matchField(Field $field) : DataTypeHandlerInterface {
    switch($field->schemaType) {
      case SchemaType::Struct:
        return new StructureDataTypeHandler();
      case SchemaType::Map:
        return new MapDataTypeHandler();
      case SchemaType::List:
        return new ListDataTypeHandler();
      case SchemaType::Data:
        // match via CLR type?
        // print_r($field);
        if($field instanceof DataField) {
          return static::matchType($field->dataType);
        }
        // return static::matchType($field->type); // TODO: wrong.
      default:
        throw new \LogicException('Not implemented');
        // throw OtherExtensions.NotImplemented($"matching {field.SchemaType}");
    }
  }



  // public static function matchType() : DataTypeHandlerInterface {
  //
  // }

  /**
   * [matchType description]
   * @param  [type]                   $type [description]
   * @return DataTypeHandlerInterface|null       [description]
   */
  public static function matchType($type) : ?DataTypeHandlerInterface {
    foreach(static::$allDataTypes as $dt) {
      if($dt->getDataType() === $type) {
        return $dt;
      }
    }
    return null;
  }

  /**
   * [matchPhpType description]
   * @param  [type] $type [description]
   * @return DataTypeHandlerInterface|null       [description]
   */
  public static function matchPhpType($type) : ?DataTypeHandlerInterface  {
    foreach(static::$allDataTypes as $dt) {
      if(isset($dt->phpType) && $dt->phpType === $type) {
        return $dt;
      }
    }
    return null;
  }

  /**
   * [matchPhpClass description]
   * @param  [type] $class [description]
   * @return DataTypeHandlerInterface|null       [description]
   */
  public static function matchPhpClass($class) : ?DataTypeHandlerInterface  {
    foreach(static::$allDataTypes as $dt) {
      if(isset($dt->phpClass) && $dt->phpClass === $class) {
        return $dt;
      }
    }
    return null;
  }

  /**
   * [ThrowClrTypeNotSupported description]
   * @param [type] $type [description]
   */
  public static function ThrowClrTypeNotSupported($type): void
  {
    $availableTypes = implode(', ', array_map(function(DataTypeHandlerInterface $dt) {
      return isset($dt->phpType) ? $dt->phpType : null; // or class?
    }, static::$allDataTypes));

    $message = "PHP type/class '{$type}' is not supported, please specify one of '{$availableTypes}' or use an alternative constructor/static creation method";

    throw new NotSupportedException($message);
  }
}

// Make sure we perform the pseudo-static class init.
DataTypeFactory::initialize();
