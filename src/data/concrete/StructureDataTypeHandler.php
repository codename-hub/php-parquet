<?php
namespace codename\parquet\data\concrete;

use Exception;

use codename\parquet\data\Field;
use codename\parquet\data\SchemaType;
use codename\parquet\data\StructField;
use codename\parquet\data\DataTypeFactory;
use codename\parquet\data\NonDataDataTypeHandler;

use codename\parquet\format\SchemaElement;
use codename\parquet\format\FieldRepetitionType;

class StructureDataTypeHandler extends NonDataDataTypeHandler
{
  /**
   * @inheritDoc
   */
  public function isMatch(
    \codename\parquet\format\SchemaElement $tse,
    ?\codename\parquet\ParquetOptions $formatOptions
  ): bool {
    return $tse->num_children > 0;
  }

  /**
   * @inheritDoc
   */
  public function createSchemaElement(
    array $schema,
    int &$index,
    int &$ownedChildCount
  ): Field {
    $container = $schema[$index++];

    $ownedChildCount = $container->num_children; //make then owned to receive in .Assign()
    $f = StructField::CreateWithNoElements($container->name);
    return $f;
  }

  /**
   * @inheritDoc
   */
  public function getSchemaType(): int
  {
    return SchemaType::Struct;
  }

  /**
   * @inheritDoc
   */
  public function createThrift(
    \codename\parquet\data\Field $field,
    \codename\parquet\format\SchemaElement $parent,
    array &$container
  ): void {
    $tseStruct = new SchemaElement([
      'name' => $field->name,
      'repetition_type' => FieldRepetitionType::OPTIONAL,
    ]);

    $container[] = $tseStruct;
    $parent->num_children += 1;

    if($field instanceof StructField) {
      foreach($field->getFields() as $cf) {
        $handler = DataTypeFactory::matchField($cf);
        $handler->createThrift($cf, $tseStruct, $container);
      }
    } else {
      throw new Exception('invalid field');
    }
  }
}
