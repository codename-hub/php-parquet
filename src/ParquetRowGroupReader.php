<?php
namespace jocoon\parquet;

use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataColumn;

use jocoon\parquet\file\ThriftFooter;
use jocoon\parquet\file\ThriftStream;
use jocoon\parquet\file\DataColumnReader;

use jocoon\parquet\format\RowGroup;
use jocoon\parquet\format\ColumnChunk;

use jocoon\parquet\helper\ThriftExtensions;

/**
 * [ParquetRowGroupReader description]
 */
class ParquetRowGroupReader {

  /**
   * [$pathToChunk description]
   * @var ColumnChunk[]
   */
  protected $pathToChunk = [];

  /**
   * [$_rowGroup description]
   * @var RowGroup
   */
  protected $_rowGroup;

  /**
   * [protected description]
   * @var ThriftFooter
   */
  protected $footer;

  /**
   * [protected description]
   * @var resource
   */
  protected $stream;

  /**
   * [protected description]
   * @var ThriftStream
   */
  protected $_thriftStream;

  /**
   * [protected description]
   * @var ParquetOptions
   */
  protected $parquetOptions;

  /**
   * [__construct description]
   * @param RowGroup        $rowGroup       [description]
   * @param ThriftFooter    $footer         [description]
   * @param resource        $stream         [description]
   * @param ThriftStream    $thriftStream   [description]
   * @param ParquetOptions  $parquetOptions [description]
   */
  public function __construct(RowGroup $rowGroup, ThriftFooter $footer, $stream, ThriftStream $thriftStream, ParquetOptions $parquetOptions)
  {
    $this->_rowGroup = $rowGroup;
    $this->footer = $footer;
    $this->stream = $stream;
    $this->_thriftStream = $thriftStream;
    $this->parquetOptions = $parquetOptions;

    foreach($this->_rowGroup->columns as $thriftChunk) {
      $path = ThriftExtensions::GetPath($thriftChunk);
      $this->pathToChunk[$path] = $thriftChunk;
    }
  }

  /**
   * [getRowCount description]
   * @return int [description]
   */
  public function getRowCount(): int {
    return $this->_rowGroup->num_rows;
  }


  /**
   * [ReadColumn description]
   * @param  DataField  $field [description]
   * @return DataColumn        [description]
   */
  public function ReadColumn(DataField $field) : DataColumn
  {
    // if ($field == null) throw new ArgumentNullException(nameof(field));

    $columnChunk = $this->pathToChunk[$field->path] ?? null;

    if($columnChunk === null) {
      throw new ParquetException("'{$field->path}' does not exist in this file");
    }

    $columnReader = new DataColumnReader($field, $this->stream, $columnChunk, $this->footer, $this->parquetOptions);

    return $columnReader->read();
  }

}
