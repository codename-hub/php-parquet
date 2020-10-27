<?php
namespace jocoon\parquet;

use Exception;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataColumn;

use jocoon\parquet\file\ThriftFooter;

use jocoon\parquet\format\FileMetaData;

/**
 * Parquet file format reader implementation
 */
class ParquetReader extends ParquetActor {

  /**
   * [$_parquetOptions description]
   * @var ParquetOptions
   */
  protected $_parquetOptions = null;

  /**
   * [$_meta description]
   * @var FileMetaData
   */
  protected $_meta = null;

  /**
   * [protected description]
   * @var ThriftFooter
   */
  protected $_footer = null;

  /**
   * [__construct description]
   * @param [type] $input          [description]
   * @param [type] $parquetOptions [description]
   */
  public function __construct($input, $parquetOptions = null)
  {
    parent::__construct($input);

    if(!is_resource($input)) {
      // not a resource, cannot open?
      throw new \Exception('Not a stream');
    }

    $streamMetadata = stream_get_meta_data($input);

    if(!$streamMetadata['seekable'] /* || strpos($streamMetadata['mode'], 'r') !== 0*/ ) {
      // TODO: check for readability?
      // print_r($streamMetadata);
      throw new \Exception('Stream must be readable and seekable');
    }

    if(fstat($input)['size'] <= 8) throw new Exception("not a Parquet file (size too small)");

    $this->ValidateFile();
    $this->_parquetOptions = $parquetOptions ?? new ParquetOptions();

    //read metadata instantly, now
    $this->_meta = $this->ReadMetadata();

    $this->_footer = new ThriftFooter($this->_meta);

    $this->InitRowGroupReaders();
    $this->init();
  }

  /**
   * internal initiation function,
   * in addition to constructor
   * @return void
   */
  protected function init() {
    $this->schema = $this->_footer->CreateModelSchema($this->_parquetOptions);
  }

  /**
   * [getRowGroupCount description]
   * @return int [description]
   */
  public function getRowGroupCount() : int {
    return count($this->_meta->row_groups);
  }

  /**
   * [getThriftMetadata description]
   * @return FileMetaData [description]
   */
  public function getThriftMetadata() : FileMetaData {
    return $this->_meta;
  }

  /**
   * [public description]
   * @var Schema
   */
  public $schema;

  /**
   * [OpenRowGroupReader description]
   * @param  int                   $index [description]
   * @return ParquetRowGroupReader        [description]
   */
  public function OpenRowGroupReader(int $index) : ParquetRowGroupReader {
    return $this->_groupReaders[$index];
  }

  /**
   * [ReadEntireRowGroup description]
   * @param  int          $rowGroupIndex [description]
   * @return DataColumn[]
   */
  public function ReadEntireRowGroup(int $rowGroupIndex = 0) : array
  {
    $dataFields = $this->schema->getDataFields();
    $reader = $this->OpenRowGroupReader($rowGroupIndex);
    $result = [];

    foreach($dataFields as $dataField) {
      $column = $reader->ReadColumn($dataField);
      $result[] = $column;
    }

    return $result;
  }

  /**
   * [protected description]
   * @var ParquetRowGroupReader[]
   */
  protected $_groupReaders = [];

  /**
   * [InitRowGroupReaders description]
   */
  protected function InitRowGroupReaders(): void
  {
    $this->_groupReaders = [];

    foreach($this->_meta->row_groups as $thriftRowGroup) {
      $this->_groupReaders[] = new ParquetRowGroupReader($thriftRowGroup, $this->_footer, $this->_fileStream, $this->ThriftStream, $this->_parquetOptions);
    }
  }


}
