<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use Exception;

use codename\parquet\data\Schema;
use codename\parquet\data\DataType;
use codename\parquet\data\MapField;
use codename\parquet\data\DataField;
use codename\parquet\data\ListField;
use codename\parquet\data\DataColumn;
use codename\parquet\data\StructField;

use codename\parquet\exception\NotSupportedException;

final class SchemaTest extends TestBase
{
  /**
   * [testCreatingElementWithUnsupportedTypeThrowsException description]
   */
  public function testCreatingElementWithUnsupportedTypeThrowsException(): void
  {
    $this->expectException(NotSupportedException::class);
    DataField::createFromType('e', 'enum'); // this doesn't even exist in PHP.
  }

  /**
   * [testSchemaElementAreEqual description]
   */
  public function testSchemaElementAreEqual(): void
  {
    // NOTE: as PHP doesn't support equality operator overrides,
    // we might use ::Equals() method instead - or in addition?
    $this->assertEquals(DataField::createFromType('id', 'integer'), DataField::createFromType('id', 'integer'));
  }

  /**
   * [testSchemaElementDifferentNamesNotEqual description]
   */
  public function testSchemaElementDifferentNamesNotEqual(): void
  {
    $this->assertNotEquals(DataField::createFromType('id1', 'integer'), DataField::createFromType('id', 'integer'));
  }

  /**
   * [testSchemaElementDifferentTypesNotEqual description]
   */
  public function testSchemaElementDifferentTypesNotEqual(): void
  {
    $this->assertNotEquals(DataField::createFromType('id', 'integer'), DataField::createFromType('id', 'double'));
  }

  /**
   * [testSchemasIdenticalEqual description]
   */
  public function testSchemasIdenticalEqual(): void
  {
    $schema1 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('city', 'string')]);
    $schema2 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('city', 'string')]);
    $this->assertEquals($schema1, $schema2);
  }

  /**
   * [testSchemasDifferentNotEqual description]
   */
  public function testSchemasDifferentNotEqual(): void
  {
    $schema1 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('city', 'string')]);
    $schema2 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('city2', 'string')]);
    $this->assertNotEquals($schema1, $schema2);
  }

  /**
   * [testSchemasDifferOnlyInRepeatedFieldsNotEqual description]
   */
  public function testSchemasDifferOnlyInRepeatedFieldsNotEqual(): void
  {
    $schema1 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('cities', 'string')]);
    $schema2 = new Schema([DataField::createFromType('id', 'integer'), DataField::createFromType('cities', 'array', 'string')]);
    $this->assertNotEquals($schema1, $schema2);
  }

  /**
   * [testSchemasDifferByNullability description]
   */
  public function testSchemasDifferByNullability(): void
  {
    // NOTE/TODO: this has to be improved
    // By default, an integer in PHP might be nullable
    // We have no strict typing here, so we have to find
    // an elegant way of expressing this.
    $notNullableField = DataField::createFromType('id', 'integer');
    $notNullableField->hasNulls = false;

    $nullableField = DataField::createFromType('id', 'integer');
    $nullableField->hasNulls = true;

    $this->assertNotEquals(
      $notNullableField,
      $nullableField
    );
  }

  /**
   * [testStringIsAlwaysNullable description]
   */
  public function testStringIsAlwaysNullable(): void
  {
    $se = DataField::createFromType('id', 'string');
    $this->assertTrue($se->hasNulls);
  }

  /**
   * [testDatetimeIsNotNullableByDefault description]
   */
  public function testDatetimeIsNotNullableByDefault(): void
  {
    // NOTE: we're using DateTimeImmutable at the moment.
    $se = DataField::createFromType('id', \DateTimeImmutable::class);
    $this->assertFalse($se->hasNulls);
  }

  //
  // NOTE: this is not applicable to PHP
  //
  // /**
  //  * [testGenericDictionariesAreNotAllowed description]
  //  */
  // public function testGenericDictionariesAreNotAllowed(): void
  // {
  //   Assert.Throws<NotSupportedException>(() => new DataField<IDictionary<int, string>>("dictionary"));
  // }

  //
  // NOTE: this is not applicable to PHP
  //
  // public void Invalid_dictionary_declaration()
  // {
  //    Assert.Throws<ArgumentException>(() => new DataField<Dictionary<int, int>>("d"));
  // }

  /**
   * [testButICanDeclareADictionary description]
   */
  public function testButICanDeclareADictionary(): void
  {
    // we do not perform assertions here.
    new MapField('dictionary', new DataField('key', DataType::Int32), new DataField('value', DataType::String));
  }

  /**
   * [testMapFieldsWithSameTypesAreEqual description]
   */
  public function testMapFieldsWithSameTypesAreEqual(): void
  {
    $this->assertEquals(
      new MapField('dictionary', new DataField('key', DataType::Int32), new DataField('value', DataType::String)),
      new MapField('dictionary', new DataField('key', DataType::Int32), new DataField('value', DataType::String))
    );
  }

  /**
   * [testMapFieldsWithDifferentTypesAreUnequal description]
   */
  public function testMapFieldsWithDifferentTypesAreUnequal(): void
  {
    $this->assertNotEquals(
      new MapField('dictionary', new DataField('key', DataType::String), new DataField('value', DataType::String)),
      new MapField('dictionary', new DataField('key', DataType::Int32), new DataField('value', DataType::String))
    );
  }

  // public function testByteArraySchemaIsABytearrayType(): void
  // {
  //   // TODO: Actually, this is not right. THis way, we're definitely getting isArray=true
  //   $field = DataField::createFromType('bytes', 'array', 'byte');
  //
  //   $this->assertEquals(DataType::ByteArray, $field->dataType);
  //   $this->assertEquals('array', $field->phpType);
  //   // Assert.Equal(typeof(byte[]), field.ClrType);
  //
  //   $this->assertFalse($field->isArray);
  //   $this->assertTrue($field->hasNulls);
  // }

  /**
   * [testCannotCreateStructWithNoElements description]
   */
  public function testCannotCreateStructWithNoElements(): void
  {
    //
    // NOTE: in this package, we're not checking arguments in the constructor
    // But instead, relying on the argument validation of php
    // Which throws a regular 'Error' exception in this case.
    //
    $this->expectException(\Error::class); // TODO
    new StructField('struct');
  }

  /**
   * [testIdenticalStructsFieldAreEqual description]
   */
  public function testIdenticalStructsFieldAreEqual(): void
  {
    $this->assertEquals(
      StructField::createWithField("f1", DataField::createFromType("name", 'string')),
      StructField::createWithField("f1", DataField::createFromType("name", 'string'))
    );
  }

  /**
   * [testStructsNamedDifferentAreNotEqual description]
   */
  public function testStructsNamedDifferentAreNotEqual(): void
  {
    $this->assertNotEquals(
      StructField::createWithField("f1", DataField::createFromType("name", 'string')),
      StructField::createWithField("f2", DataField::createFromType("name", 'string'))
    );
  }

  /**
   * [testStructsWithDifferentChildrenAreNotEqual description]
   */
  public function testStructsWithDifferentChildrenAreNotEqual(): void
  {
    $this->assertNotEquals(
      StructField::createWithField("f1", DataField::createFromType("name", 'string')),
      StructField::createWithField("f1", DataField::createFromType("name", 'integer'))
    );
  }

  /**
   * [testListsAreEqual description]
   */
  public function testListsAreEqual(): void
  {
    $this->assertEquals(
      new ListField("item", DataField::createFromType('id', 'integer')),
      new ListField("item", DataField::createFromType('id', 'integer'))
    );
  }

  /**
   * [testListsAreNotEqualByName description]
   */
  public function testListsAreNotEqualByName(): void
  {
    $this->assertNotEquals(
      new ListField("item", DataField::createFromType('id', 'integer')),
      new ListField("item1", DataField::createFromType('id', 'integer'))
    );
  }

  /**
   * [testListsAreNotEqualByItem description]
   */
  public function testListsAreNotEqualByItem(): void
  {
    $this->assertNotEquals(
      new ListField("item", DataField::createFromType('id', 'integer')),
      new ListField("item", DataField::createFromType('id', 'string'))
    );
  }

  /**

   * [testListMaintainsPathPrefix description]
   */
  public function testListMaintainsPathPrefix(): void
  {
    $list = new ListField('List', DataField::createFromType('id', 'integer'));
    $list->setPathPrefix('Parent');

    $this->assertEquals('Parent.List.list.id', $list->item->path);
  }

  /**
   * [testPlainDataField_0R_0D description]
   */
  public function testPlainDataField_0R_0D(): void
  {
    $dataField = DataField::createFromType("id", 'integer');
    $dataField->hasNulls = false; // true by default, due to PHP.

    $schema = new Schema([$dataField]);
    $this->assertEquals(0, $schema->getDataFields()[0]->maxRepetitionLevel);
    $this->assertEquals(0, $schema->getDataFields()[0]->maxDefinitionLevel);
  }

  /**
   * [testPlainNullableDataField_0R_1D description]
   */
  public function testPlainNullableDataField_0R_1D(): void
  {
    $dataField = DataField::createFromType("id", 'integer');
    // $dataField->hasNulls = true; // true by default..

    $schema = new Schema([$dataField]);
    $this->assertEquals(0, $schema->getDataFields()[0]->maxRepetitionLevel);
    $this->assertEquals(1, $schema->getDataFields()[0]->maxDefinitionLevel);
  }

  /**
   * [testMapOfRequiredKeyAndOptionalValue_1R2D_1R3D description]
   */
  public function testMapOfRequiredKeyAndOptionalValue_1R2D_1R3D(): void
  {
    $integerField = DataField::createFromType("key", 'integer');
    $integerField->hasNulls = false;

    $schema = new Schema([
      new MapField("numbers",
        $integerField,
        DataField::createFromType("value", 'string')
      )
    ]);

    $field = $schema->fields[0];

    $this->assertTrue($field instanceof MapField);

    if($field instanceof MapField) {
      $key = $field->key;
      $value = $field->value;

      $this->assertEquals(1, $key->maxRepetitionLevel);
      $this->assertEquals(2, $key->maxDefinitionLevel);

      $this->assertEquals(1, $value->maxRepetitionLevel);
      $this->assertEquals(3, $value->maxDefinitionLevel);
    }
  }

  /**
   * [testListOfStructuresValidLevels description]
   */
  public function testListOfStructuresValidLevels(): void
  {
    $idField = DataField::createFromType('id', 'integer');
    $idField->hasNulls = false; // NOTE: PHP ints are nullable, therefore we have to force not-nullable here
    $nameField = DataField::createFromType('name', 'string');
    $nameField->hasNulls = true; // NOTE: enforce nullable string

    $schema = new Schema([
      DataField::createFromType('id', 'integer'),
      new ListField("structs",
        StructField::createWithFieldArray("mystruct", [
          $idField,
          $nameField
        ])
      )
    ]);

    $this->assertEquals(1, $idField->maxRepetitionLevel);
    $this->assertEquals(2, $idField->maxDefinitionLevel);

    $this->assertEquals(1, $nameField->maxRepetitionLevel);
    $this->assertEquals(3, $nameField->maxDefinitionLevel);
  }
}
