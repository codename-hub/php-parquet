<?php
namespace jocoon\parquet;

use Exception;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataColumn;
use jocoon\parquet\data\DataTypeFactory;

use jocoon\parquet\file\ThriftFooter;
use jocoon\parquet\file\ThriftStream;
use jocoon\parquet\file\DataColumnWriter;

use jocoon\parquet\format\RowGroup;
use jocoon\parquet\format\SchemaElement;

class ParquetRowGroupWriter
{
  /**
   * [protected description]
   * @var Schema
   */
  protected $schema;

  /**
   * handle
   * @var resource
   */
  protected $stream;

  /**
   * [protected description]
   * @var ThriftStream
   */
  protected $thriftStream;

  /**
   * [protected description]
   * @var ThriftFooter
   */
  protected $footer;

  /**
   * [protected description]
   * @var int
   */
  protected $compressionMethod;

  /**
   * [protected description]
   * @var ParquetOptions
   */
  protected $formatOptions;

  /**
   * [protected description]
   * @var RowGroup
   */
  protected $thriftRowGroup;

  /**
   * [protected description]
   * @var int
   */
  protected $rgStartPos;

  /**
   * [protected description]
   * @var SchemaElement[]
   */
  protected $thschema;

  /**
   * [protected description]
   * @var int
   */
  protected $colIdx = 0;

  /**
   * [protected description]
   * @var int|null
   */
  protected $rowCount = null;

  /**
   * returns the current row count
   * @return int [description]
   */
  public function getRowCount(): int {
    return $this->rowCount;
  }

  /**
   * [__construct description]
   * @param Schema         $schema            [description]
   * @param [type]         $stream            [description]
   * @param ThriftStream   $thriftStream      [description]
   * @param ThriftFooter   $footer            [description]
   * @param int            $compressionMethod [description]
   * @param ParquetOptions $formatOptions     [description]
   */
  public function __construct(
    Schema $schema,
    $stream,
    ThriftStream $thriftStream,
    ThriftFooter $footer,
    int $compressionMethod,
    ParquetOptions $formatOptions
  ) {
    $this->schema = $schema;
    $this->stream = $stream;
    $this->thriftStream = $thriftStream;
    $this->footer = $footer;
    $this->compressionMethod = $compressionMethod;
    $this->formatOptions = $formatOptions;

    $this->thriftRowGroup = $this->footer->addRowGroup();
    $this->rgStartPos = ftell($stream);

    $this->thriftRowGroup->columns = [];
    $this->thschema = $this->footer->GetWriteableSchema();
  }

  /**
   * [WriteColumn description]
   * @param DataColumn $column [description]
   */
  public function WriteColumn(DataColumn $column)
  {
    // if (column == null) throw new ArgumentNullException(nameof(column));

    if ($this->rowCount === null)
    {
      if(count($column->getData()) > 0 || $column->getField()->maxRepetitionLevel === 0) {
        $this->rowCount = $column->calculateRowCount();
      }
    }

    // Thrift.SchemaElement tse = _thschema[_colIdx];
    $tse = $this->thschema[$this->colIdx];

    // if(!column.Field.Equals(tse))
    if(!$column->getField()->Equals($tse)) {
      throw new Exception("cannot write this column, expected '{$tse->name}', passed: '{$column->getField()->name}'");
     // TODO: throw new ArgumentException($"cannot write this column, expected '{tse.Name}', passed: '{column.Field.Name}'", nameof(column));
    }

    $dataTypeHandler = DataTypeFactory::matchSchemaElement($tse, $this->formatOptions);
    $this->colIdx += 1;

    //
    // TODO:
    $path = $this->footer->getPath($tse);

    $writer = new DataColumnWriter($this->stream, $this->thriftStream, $this->footer, $tse, $this->compressionMethod, $this->rowCount); // TODO: check row count default value?

    // Set configuration to enable/disable statistics generation
    $writer->calculateStatistics = $this->formatOptions->CalculateStatistics;

    // Thrift.ColumnChunk chunk = writer.Write(path, column, dataTypeHandler);
    $chunk = $writer->write($path, $column, $dataTypeHandler);
    $this->thriftRowGroup->columns[] = $chunk;
    // _thriftRowGroup.Columns.Add(chunk);

  }

  /**
   * Internal variable to determine finished (writing) state
   * Just for testing at the moment
   * as we have no "using"-clause like in C# and we can't rely on destructors
   * @var bool
   */
  protected $finished = false;

  /**
   * Destructor needed for finishing some metadata
   */
  public function finish() {

    // DEBUG/CHECK: custom finisher method/interception
    if($this->finished) {
      return;
    }
    $this->finished = true;

    //todo: check if all columns are present

    //row count is know only after at least one column is written
    // _thriftRowGroup.Num_rows = RowCount ?? 0;
    $this->thriftRowGroup->num_rows = $this->getRowCount() ?? 0;

    //row group's size is a sum of _uncompressed_ sizes of all columns in it, including the headers
    //luckily ColumnChunk already contains sizes of page+header in it's meta
    // _thriftRowGroup.Total_byte_size = _thriftRowGroup.Columns.Sum(c => c.Meta_data.Total_compressed_size);

    $sum = 0;
    foreach($this->thriftRowGroup->columns as $cc) {
      $sum += $cc->meta_data->total_compressed_size;
    }

    $this->thriftRowGroup->total_byte_size = $sum;
  }

}
