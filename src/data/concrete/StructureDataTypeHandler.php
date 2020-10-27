<?php
namespace jocoon\parquet\data\concrete;

use Exception;

use jocoon\parquet\data\Field;
use jocoon\parquet\data\SchemaType;
use jocoon\parquet\data\StructField;
use jocoon\parquet\data\DataTypeFactory;
use jocoon\parquet\data\NonDataDataTypeHandler;

use jocoon\parquet\format\SchemaElement;
use jocoon\parquet\format\FieldRepetitionType;

class StructureDataTypeHandler extends NonDataDataTypeHandler
{
  /**
   * @inheritDoc
   */
  public function isMatch(
    \jocoon\parquet\format\SchemaElement $tse,
    ?\jocoon\parquet\ParquetOptions $formatOptions
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
    \jocoon\parquet\data\Field $field,
    \jocoon\parquet\format\SchemaElement $parent,
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
