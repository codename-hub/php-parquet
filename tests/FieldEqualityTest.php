<?php
declare(strict_types=1);
namespace codename\parquet\tests;

use codename\parquet\data\Schema;
use codename\parquet\data\MapField;
use codename\parquet\data\DataField;
use codename\parquet\data\ListField;
use codename\parquet\data\StructField;

/**
 * Various tests for Field::Equals method
 */
class FieldEqualityTest extends TestBase
{
  /**
   * Tests simple equality
   */
  public function testDataFieldEqual(): void {
    $field1 = DataField::createFromType('sameNameSameType', 'integer');
    $field2 = DataField::createFromType('sameNameSameType', 'integer');
    $this->assertTrue($field1->Equals($field2));
  }

  /**
   * tests same named fields but differing types to be not equal
   */
  public function testDataFieldDifferingType(): void {
    $field1 = DataField::createFromType('sameNameDifferentType', 'integer');
    $field2 = DataField::createFromType('sameNameDifferentType', 'string');
    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * tests differing field names are recognized as inequal
   */
  public function testDataFieldDifferingFieldName(): void {
    $field1 = DataField::createFromType('sameNameSameType', 'integer');
    $field2 = DataField::createFromType('differingFieldName', 'integer');
    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * Tests field inequality by nullability flag
   */
  public function testDataFieldDifferingNullability(): void {
    $field1 = DataField::createFromType('sameNameSameTypeDifferingNullability', 'integer');
    $field2 = DataField::createFromType('sameNameSameTypeDifferingNullability', 'integer');
    $field1->hasNulls = true;
    $field2->hasNulls = false;
    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * tests field inequality with a lists' element field
   */
  public function testDataFieldDifferingPathByList(): void {
    $field1 = DataField::createFromType('sameNameButDifferingPath', 'integer');
    $field2 = DataField::createFromType('sameNameButDifferingPath', 'integer');

    // nest a field in a list, causes differing path
    $list = new ListField('aListField', $field2);

    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * Tests field inequality with a field in a struct
   */
  public function testDataFieldDifferingPathByStruct(): void {
    $field1 = DataField::createFromType('sameNameButDifferingPath', 'integer');
    $field2 = DataField::createFromType('sameNameButDifferingPath', 'integer');

    // nest a field in a struct, causes differing path
    $struct = StructField::createWithField('aStructField', $field2);

    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * Tests field inqeuality for a map's key field
   */
  public function testDataFieldDifferingPathByMapKey(): void {
    $field1 = DataField::createFromType('sameNameButDifferingPath', 'integer');
    $field2 = DataField::createFromType('sameNameButDifferingPath', 'integer');

    // nest a field in a maps' key field, causes differing path
    $struct = new MapField(
      'aMapField',
      $field2,
      DataField::createFromType('irrelevantMapValue', 'integer') // dummy field
    );

    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * Tests field inequality for a map's value field
   */
  public function testDataFieldDifferingPathByMapValue(): void {
    $field1 = DataField::createFromType('sameNameButDifferingPath', 'integer');
    $field2 = DataField::createFromType('sameNameButDifferingPath', 'integer');

    // nest a field in a maps' value field, causes differing path
    $struct = new MapField(
      'aMapField',
      DataField::createFromType('irrelevantMapValue', 'integer'), // dummy field
      $field2
    );

    $this->assertFalse($field1->Equals($field2));
  }

  /**
   * Tests various field (in)equalities and nestings
   */
  public function testFieldEqual(): void {

    // prepare a dummy field.
    $dummyField = DataField::createFromType('dummy', 'integer');

    $nestedInListField = DataField::createFromType('aField', 'integer');
    $listField = new ListField('aListField', $nestedInListField);

    $nestedInStructField = DataField::createFromType('aField', 'integer');
    $structField = StructField::createWithField('aStructField', $nestedInStructField);

    $nestedInMapKeyField = DataField::createFromType('aField', 'integer');
    $mapField1 = new MapField('aMapField', $nestedInMapKeyField, $dummyField);

    $nestedInValueField = DataField::createFromType('aField', 'integer');
    $mapField2 = new MapField('aMapField', $dummyField, $nestedInValueField);

    // all same-named fields
    // but all differing kinds
    $fields = [
      DataField::createFromType('aField', 'integer'),
      new MapField('aField', $dummyField, $dummyField),
      StructField::createWithField('aField', $dummyField),
      new ListField('aField', $dummyField),
      $nestedInListField,
      $nestedInStructField,
      $nestedInMapKeyField,
      $nestedInValueField,
    ];

    foreach($fields as $outerFieldIndex => $field1) {
      foreach($fields as $innerFieldIndex => $field2) {
        if($outerFieldIndex === $innerFieldIndex) {
          $this->assertTrue($field1->Equals($field2), "($outerFieldIndex, $innerFieldIndex) Fields must be determined as equal: {$field1->schemaType} <=> {$field2->schemaType} - type field1: ".get_class($field1)." type field2: ".get_class($field2));
        } else {
          $this->assertFalse($field1->Equals($field2), "($outerFieldIndex, $innerFieldIndex) Fields must be determined as inequal: {$field1->schemaType} <=> {$field2->schemaType} - type field1: ".get_class($field1)." type field2: ".get_class($field2));
        }
      }
    }

  }
}
