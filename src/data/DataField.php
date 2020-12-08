<?php
namespace jocoon\parquet\data;

use jocoon\parquet\helper\OtherExtensions;

class DataField extends Field
{
  /**
   * [public description]
   * @var DataType
   */
  public $dataType;

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
   * [public description]
   * @var string
   */
  public $phpType;

  /**
   * Class FQDN, if applicable
   * @var string
   */
  public $phpClass;

  public function __construct(string $name, int $dataType, bool $hasNulls = true, bool $isArray = false)
  {
    parent::__construct($name, SchemaType::Data);
    $this->dataType = $dataType;
    $this->hasNulls = $hasNulls;
    $this->isArray = $isArray;

    $this->maxRepetitionLevel = $isArray ? 1 : 0;

    $handler = DataTypeFactory::matchType($dataType);
    if($handler !== null) {
      $this->phpType = $handler->phpType;
      $this->phpClass = $handler->phpClass;
      // $this->phpTypeNullableIfHasNullsType = $hasNulls ? ... : $this->phpType;
    }
  }


  /**
   * [createFromType description]
   * @param  string    $name           [name of the datafield]
   * @param  string    $type           [a PHP type]
   * @param  [type]    $enumerableType [if type == array, this specifies the underlying type]
   * @return DataField                 [description]
   */
  public static function createFromType(string $name, string $type, $enumerableType = null) : DataField {

    $dataType = null;

    $hasNulls = null; // NOTE: not set from the start
    $isArray = null;  // NOTE: not set from the start

    $baseType = null;
    if($type == 'array') {
      $baseType = $enumerableType;
      $isArray = true;
      $hasNulls = false; // TODO/CHECK if this is the default state for arrays...
    } else {
      $baseType = $type;
      // TODO: throw exception on enumerableType not null?
    }


    $handler = null;

    switch($baseType) {
      case 'string':
        $dataType = DataType::String;
        break;
      case 'integer':
        $dataType = DataType::Int32; // or 64?
        break;
      case 'long': // Faking it
        $baseType = 'integer';
        $dataType = DataType::Int64; // or 64?
        break;
      case 'boolean':
        $dataType = DataType::Boolean;
        break;
      case 'decimal':
        // NOTE: PHP has no explicit decimal type
        // Instead, we use strings, so we can use these arbitrary precision numbers
        // with libaries like BCMath
        // Therefore, we have to hook into this thing right now to get the right DataTypeHandler
        $baseType = 'string';
        $dataType = DataType::Decimal;
        $handler = DataTypeFactory::matchType($dataType);
        break;
      case 'float':
        // NOTE: PHP is using doubles, internally - at any time possible.
        // Therefore, we have to hook into this thing right now to get the right DataTypeHandler
        $baseType = 'double';
        $dataType = DataType::Float;
        $handler = DataTypeFactory::matchType($dataType);
        break;
      case 'double':
        $dataType = DataType::Double;
        break;
      case 'byte':
        // TODO/WIP: ByteArrays seem to be a little bit trickier
        // As PHP uses bare strings to store bytes - or at least, supports it this way.
        if($isArray) {
          $dataType = DataType::ByteArray;
          $isArray = false; // ByteArrayDataHandler already incorporates this
        } else {
          $dataType = DataType::Byte;
        }
        $handler = DataTypeFactory::matchType($dataType);
        break;

    }

    if($handler === null) {
      $handler = DataTypeFactory::matchPhpType($baseType);
    }

    if($handler === null) {
      $handler = DataTypeFactory::matchPhpClass($baseType);

      if($handler !== null) {
        // inherit DataType from handler
        if($dataType === null) {
          $dataType = $handler->getDataType();
        }

        // DateTimeOffset should not be nullable by default
        if($dataType === DataType::DateTimeOffset) {
          $hasNulls = false;

          // NOTE/TODO default fallback for DateTimeDataField ?
          return \jocoon\parquet\data\DateTimeDataField::create(
            $name,
            \jocoon\parquet\data\DateTimeFormat::Impala,
            $hasNulls,
            $isArray ?? false
          );
        }
      }

    }

    if ($handler === null) DataTypeFactory::ThrowClrTypeNotSupported($baseType);

    // TODO: throw an exception, if only one of $isArray or $hasNulls is set, but not both

    if($isArray !== null && $hasNulls !== null) {
      return new DataField($name, $dataType, $hasNulls, $isArray);
    } else if($hasNulls !== null) {
      return new DataField($name, $dataType, $hasNulls);
    } else {
      return new DataField($name, $dataType /*, $hasNulls, $isArray*/);
    }

  }


  /**
   * @inheritDoc
   */
  public function PropagateLevels(
    int $parentRepetitionLevel,
    int $parentDefinitionLevel
  ): void {
    $this->maxRepetitionLevel = $parentRepetitionLevel + ($this->isArray ? 1 : 0);
    $this->maxDefinitionLevel = $parentDefinitionLevel + ($this->hasNulls ? 1 : 0);
  }

  /**
   * @inheritDoc
   */
  public function setPathPrefix($value)
  {
    $this->path = OtherExtensions::AddPath($value, $this->name);
  }

  /**
   * Equality check
   * NOTE: as there's no method overriding for equality operators in PHP
   * This has to be called manually
   *
   * @param  [type] $other [description]
   * @return bool          [description]
   */
  public function Equals($other) : bool
  {
    if ($other === null) return false;
    if ($other === $this) return true;

    //todo: check equality for child elements
    if($other instanceof DataField) {
      return
        $this->name === $other->name &&
        $this->dataType === $other->dataType &&
        $this->hasNulls === $other->hasNulls &&
        $this->isArray  === $other->isArray;
    } else {
      //
      // NOTE: we're calling Field::Equals, which allows comparing SchemaElement objects
      //
      return parent::Equals($other);
    }
  }

}
