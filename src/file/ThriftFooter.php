<?php
namespace jocoon\parquet\file;

use jocoon\parquet\ParquetOptions;

use jocoon\parquet\data\Field;
use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataFactory;
use jocoon\parquet\data\DataTypeFactory;

use jocoon\parquet\format\Encoding;
use jocoon\parquet\format\PageType;
use jocoon\parquet\format\RowGroup;
use jocoon\parquet\format\PageHeader;
use jocoon\parquet\format\Statistics;
use jocoon\parquet\format\ColumnChunk;
use jocoon\parquet\format\FileMetaData;
use jocoon\parquet\format\SchemaElement;
use jocoon\parquet\format\ColumnMetaData;
use jocoon\parquet\format\DataPageHeader;
use jocoon\parquet\format\FieldRepetitionType;

class ThriftFooter {

  /**
   * [protected description]
   * @var FileMetaData
   */
  protected $fileMeta = null;

  /**
   * [protected description]
   * @var ThriftSchemaTree
   */
  protected $tree;

  /**
   * [__construct description]
   * @param FileMetaData|null $fileMeta [description]
   */
  public function __construct(?FileMetaData $fileMeta)
  {
    $this->fileMeta = $fileMeta;
    if($fileMeta) {
      // only construct ThriftSchemaTree, if we already have a FileMetaData object
      $this->tree = new ThriftSchemaTree($this->fileMeta->schema);
    }
  }

  /**
   * [createFromSchema description]
   * @param  Schema       $schema        [description]
   * @param  int          $totalRowCount [description]
   * @return ThriftFooter                [description]
   */
  public static function createFromSchema(Schema $schema, int $totalRowCount): ThriftFooter {
    $instance = new self(null);
    $instance->fileMeta = $instance->CreateThriftSchema($schema);
    $instance->fileMeta->num_rows = $totalRowCount;
    $instance->fileMeta->created_by = 'php-parquet'; // TODO add version identifier
    $instance->tree = new ThriftSchemaTree($instance->fileMeta->schema);
    return $instance;
  }

  /**
   * [CreateThriftSchema description]
   * @param  Schema       $schema [description]
   * @return FileMetaData         [description]
   */
  public function CreateThriftSchema(Schema $schema): FileMetaData
  {
     $meta = new FileMetaData();

     $meta->row_groups = [];
     $meta->num_rows = 0;

     $meta->version = 1;
     $meta->schema = [];

     $root = $this->AddRoot($meta->schema);
     $this->CreateThriftSchemaInternal($schema->fields, $root, $meta->schema);

     return $meta;
  }

  /**
   * [AddRoot description]
   * @param  SchemaElement[]         &$container [description]
   * @return SchemaElement           [description]
   */
  protected function AddRoot(array &$container): SchemaElement
  {
    $root = new SchemaElement(['name' => 'php_parquet_schema']);
    $container[] = $root;
    return $root;
  }

  /**
   * [CreateThriftSchemaInternal description]
   * @param Field[]             &$ses       [description]
   * @param SchemaElement       $parent    [description]
   * @param SchemaElement[]     &$container [description]
   */
  protected function CreateThriftSchemaInternal(array &$ses, SchemaElement $parent, array &$container): void
  {
    foreach($ses as $se) {
      $handler = DataTypeFactory::matchField($se);
      $handler->createThrift($se, $parent, $container);
    }
  }

  /**
   * [add description]
   * @param int $totalRowCount [description]
   */
  public function add(int $totalRowCount)
  {
    $this->fileMeta->num_rows += $totalRowCount;
  }

  /**
   * [write description]
   * @param  ThriftStream $thriftStream [description]
   * @return int                        [description]
   */
  public function write(ThriftStream $thriftStream): int
  {
     return $thriftStream->Write(FileMetaData::class, $this->fileMeta);
  }

  /**
   * [GetSchemaElement description]
   * @param  ColumnChunk   $columnChunk [description]
   * @return SchemaElement              [description]
   */
  public function GetSchemaElement(ColumnChunk $columnChunk) : SchemaElement
  {
     // if ($columnChunk == null)
     // {
     //    throw new ArgumentNullException(nameof(columnChunk));
     // }

     // List<string> path = columnChunk.Meta_data.Path_in_schema;

     $path = $columnChunk->meta_data->path_in_schema;

     $i = 0;
     foreach ($path as $pp)
     {
        while($i < count($this->fileMeta->schema))
        {
           if ($this->fileMeta->schema[$i]->name == $pp) {
             break;
           }

           $i++;
        }
     }

     return $this->fileMeta->schema[$i];
  }

  public function CreateModelSchema(?ParquetOptions $formatOptions) : Schema {
    $si = 0;
    $tse = $this->fileMeta->schema[$si++];
    $container = [];

    $this->_CreateModelSchema(null, $container, $tse->num_children, $si, $formatOptions);

    return new Schema($container);
  }

  protected function _CreateModelSchema(?string $path, array &$container, int $childCount, int &$si, ?ParquetOptions $formatOptions) {
    for ($i=0; $i < $childCount && $si < count($this->fileMeta->schema); $i++) {
      $tse = $this->fileMeta->schema[$si];

      // // $dth = DataTypeFactory::match($tse, $formatOptions);
      $dth = DataTypeFactory::matchSchemaElement($tse, $formatOptions);

      if($dth === null) {
        throw new \Exception('no DTH');
      }

      // if(!$dth) {
      //   continue;
      // }

      $ownedChildCount = 0;

      $se = $dth->createSchemaElement($this->fileMeta->schema, $si, $ownedChildCount);

      // set $se->path !
      $se->path = implode(Schema::PathSeparator, array_filter([$path, $se->path ?? $se->name ]));

      // print_r($se);
      // die();

      // $se = new DataField($tse->name, $type);

      if($ownedChildCount > 0) {
        $childContainer = [];
        $this->_CreateModelSchema($se->path, $childContainer, $ownedChildCount, $si, $formatOptions);
        foreach($childContainer as $cse) {
          // TODO recursion? / CHANGED 2020-10-12: should be ok now.
          $se->assign($cse);
        }
      }

      $container[] = $se;
    }

    // die();
  }

  /**
   * [GetLevels description]
   * @param ColumnChunk $columnChunk        [description]
   * @param int         &$maxRepetitionLevel [description]
   * @param int         &$maxDefinitionLevel [description]
   */
  public function GetLevels(ColumnChunk $columnChunk, int &$maxRepetitionLevel, int &$maxDefinitionLevel)
  {
    $maxRepetitionLevel = 0;
    $maxDefinitionLevel = 0;

    $i = 0;
    $path = $columnChunk->meta_data->path_in_schema;
    $fieldCount = \count($this->fileMeta->schema);

    foreach ($path as $pp)
    {
      while($i < $fieldCount)
      {
        if($this->fileMeta->schema[$i]->name == $pp)
        {
          $se = $this->fileMeta->schema[$i];

          $repeated = ($se->repetition_type !== null && $se->repetition_type == FieldRepetitionType::REPEATED);
          $defined = ($se->repetition_type == FieldRepetitionType::REQUIRED);

          if ($repeated) $maxRepetitionLevel += 1;
          if (!$defined) $maxDefinitionLevel += 1;

          break;
        }

        $i++;
      }
    }
   }

  // /**
  //  *
  //  */
  // public function __construct($schema, int $totalRowCount)
  // {
  //
  // }

  /**
   * [addRowGroup description]
   * @return RowGroup [description]
   */
  public function addRowGroup(): RowGroup
  {
    $rg = new RowGroup();
    if ($this->fileMeta->row_groups === null) $this->fileMeta->row_groups = [];
    $this->fileMeta->row_groups[] = $rg;
    return $rg;
  }

  /**
   * [GetPath description]
   * @param  SchemaElement $schemaElement [description]
   * @return string[]                     [description]
   */
  public function GetPath(SchemaElement $schemaElement): array
  {
    $path = [];

    // $tree = new ThriftSchemaTree($this->fileMeta->schema);
    $tree = $this->tree;
    $wrapped = $tree->Find($schemaElement);

    while($wrapped->parent !== null)
    {
      $path[] = $wrapped->element->name;
      $wrapped = $wrapped->parent;
    }

    $path = array_reverse($path);

    return $path;
  }

  /**
   * [GetWriteableSchema description]
   * @return SchemaElement[] [description]
   */
  public function GetWriteableSchema(): array
  {
    return array_values(array_filter($this->fileMeta->schema, function(SchemaElement $tse) { return $tse->type !== null /*isset($tse->type)*/; })); // .Schema.Where(tse => tse.__isset.type).ToArray();
  }


  /**
   * [CreateColumnChunk description]
   * @param  int         $compression [description]
   * @param  [type]      $output      [description]
   * @param  [type]      $columnType  [description]
   * @param  array       $path        [description]
   * @param  int         $valuesCount [description]
   * @return ColumnChunk              [description]
   */
  public function CreateColumnChunk(int $compression, $output, $columnType, array $path, int $valuesCount): ColumnChunk
  {
    $codec = DataFactory::GetThriftCompression($compression);

    $chunk = new ColumnChunk();
    $startPos = ftell($output);
    $chunk->file_offset = $startPos;
    $chunk->meta_data = new ColumnMetaData();
    $chunk->meta_data->num_values = $valuesCount;
    $chunk->meta_data->type = $columnType;
    $chunk->meta_data->codec = $codec;
    $chunk->meta_data->data_page_offset = $startPos;
    $chunk->meta_data->encodings = [
      Encoding::RLE,
      Encoding::BIT_PACKED,
      Encoding::PLAIN,
    ];
    $chunk->meta_data->path_in_schema = $path;
    $chunk->meta_data->statistics = new Statistics();

    return $chunk;
  }

  /**
   * [CreateDataPage description]
   * @param  int        $valueCount [description]
   * @return PageHeader             [description]
   */
  public function CreateDataPage(int $valueCount): PageHeader
  {
    $ph = new PageHeader([
      'type'                    => PageType::DATA_PAGE,
      'uncompressed_page_size'  => 0,
      'compressed_page_size'    => 0,
    ]);
    $ph->data_page_header = new DataPageHeader([
      'encoding' => Encoding::PLAIN,
      'definition_level_encoding' => Encoding::RLE,
      'repetition_level_encoding' => Encoding::RLE,
      'num_values' => $valueCount,
      'statistics' => new Statistics(),
    ]);

    return $ph;
  }

}


class Node
{
  /**
   * [public description]
   * @var SchemaElement
   */
  public $element;

  /**
   * [public description]
   * @var Node[]
   */
  public $children;

  /**
   * [public description]
   * @var Node
   */
  public $parent;
}

/**
 * [ThriftSchemaTree description]
 */
class ThriftSchemaTree
{
  /**
   * [public description]
   * @var Node
   */
  public $root;

  // /**
  //  * [protected description]
  //  * @var \SplObjectStorage
  //  */
  // protected $memoizedFindResults;

  /**
   * [__construct description]
   * @param SchemaElement[] $schema [description]
   */
  public function __construct(array $schema)
  {
    // NOTE: SplObjectStorage might not be the right choice
    // for caching tree traversal results. It yields worse results.
    // $this->memoizedFindResults = new \SplObjectStorage();
    $this->root = new Node();
    $this->root->element = $schema[0];
    $i = 1;

    $this->BuildSchema($this->root, $schema, $this->root->element->num_children, $i);
  }

  /**
   * [Find description]
   * @param  SchemaElement $tse [description]
   * @return Node               [description]
   */
  public function Find(SchemaElement $tse): Node
  {
    // NOTE: see constructor note
    // if($this->memoizedFindResults->contains($tse)) {
    //   return $this->memoizedFindResults->offsetGet($tse);
    // }
    $node = $this->FindInternal($this->root, $tse);
    // NOTE: see constructor note
    // $this->memoizedFindResults->offsetSet($tse, $node);
    return $node;
  }

  /**
   * [FindInternal description]
   * @param  Node          $root [description]
   * @param  SchemaElement $tse  [description]
   * @return Node|null           [description]
   */
  private function FindInternal(Node $root, SchemaElement $tse): ?Node
  {
    foreach($root->children as $child)
    {
      if ($child->element === $tse) return $child;

      if($child->children !== null)
      {
        $cf = $this->FindInternal($child, $tse);
        if ($cf !== null) return $cf;
      }
    }

    return null;
  }

  /**
   * [BuildSchema description]
   * @param Node            $parent [description]
   * @param SchemaElement[] $schema [description]
   * @param int             $count  [description]
   * @param int             &$i      [description]
   */
  private function BuildSchema(Node $parent, array $schema, int $count, int &$i): void
  {
    $parent->children = [];

    for($ic = 0; $ic < $count; $ic++)
    {
      $child = $schema[$i++];

      $node = new Node();
      $node->element = $child;
      $node->parent = $parent;

      $parent->children[] = $node;

      if($child->num_children > 0) {
        $this->BuildSchema($node, $schema, $child->num_children, $i);
      }
    }
  }
}
