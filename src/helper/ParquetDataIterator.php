<?php
namespace codename\parquet\helper;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetOptions;
use codename\parquet\ParquetRowGroupReader;

use codename\parquet\data\DataField;

/**
 * Iterator for reading a parquet file
 * and all of its row groups, data chunks and data pages
 */
class ParquetDataIterator implements \Iterator, \Countable
{
  /**
   * Current index/row
   * @var int
   */
  protected $position = 0;

  /**
   * [protected description]
   * @var int
   */
  protected $currentRowGroupIndex = 0;

  /**
   * Current row group reader instance
   * @var ParquetRowGroupReader
   */
  protected $currentRowGroupReader;

  /**
   * Reader instance
   * @var ParquetReader
   */
  protected $reader;

  /**
   * [protected description]
   * @var DataField[]
   */
  protected $dataFields;

  /**
   * [__construct description]
   * @param ParquetReader $reader  [description]
   */
  public function __construct(ParquetReader $reader)
  {
    $this->reader = $reader;
    $this->dataFields = $this->reader->schema->getDataFields();
    $this->setCalculatedCount();
  }

  /**
   * [protected description]
   * @var int
   */
  protected $calculatedCount;

  /**
   * Pre-evaluate count of rows and store this value
   */
  protected function setCalculatedCount(): void {
    $this->calculatedCount = $this->reader->getRowCount();
  }

  /**
   * [fromFile description]
   * @param  string                 $file     [Path to parquet file]
   * @param  ParquetOptions|null    $options  [Options]
   * @return self
   */
  public static function fromFile(string $file, ?ParquetOptions $options = null): self {
    $stream = fopen($file, 'r');
    return static::fromHandle($stream, $options);
  }

  /**
   * [fromHandle description]
   * @param  resource             $stream     [File handle/stream of parquet file]
   * @param  ParquetOptions|null  $options    [Options]
   * @return self
   */
  public static function fromHandle($stream, ?ParquetOptions $options = null): self {
    $reader = new ParquetReader($stream, $options);
    return new self($reader);
  }

  /**
   * @inheritDoc
   */
  public function current()
  {
    return $this->currentBuffer[$this->offsetPosition];
  }

  /**
   * Current buffer of read entries
   * @var array
   */
  protected $currentBuffer;

  /**
   * [read description]
   */
  protected function read(): void {
    // If we exceed the count/index of available Row Group readers
    // we simply kill it right here.
    if($this->currentRowGroupIndex > $this->reader->getRowGroupCount()) {
      $this->lastCount = -1;
      $this->offsetPosition = 0;
      $this->currentBuffer = null;
      $this->currentRowGroupReader = null;
      return;
    }

    $this->currentRowGroupReader = $this->reader->OpenRowGroupReader($this->currentRowGroupIndex);

    $iterableColumns = [];
    foreach($this->dataFields as $df) {
      $columnReader = $this->currentRowGroupReader->getDataColumnReader($df);
      $iterableColumns[] = $columnReader->getDatacolumnIterable();
    }

    $arrayConverter = new DataColumnsToArrayConverter($this->reader->schema, $iterableColumns);
    $this->currentBuffer = $arrayConverter->toArray();

    $this->lastCount = count($this->currentBuffer);
    $this->totalReadCount += $this->lastCount;
    $this->offsetPosition = 0;

    // inc to the next row group
    // for the next call to read()
    $this->currentRowGroupIndex++;
  }

  /**
   * read entries (total - across row groups)
   * @var int
   */
  protected $totalReadCount = 0;

  /**
   * Count of entries recently read
   * @var int
   */
  protected $lastCount = 0;

  /**
   * Current position in the current buffer
   * @var int
   */
  protected $offsetPosition = 0;

  /**
   * @inheritDoc
   */
  public function next()
  {
    $this->position++;
    $this->offsetPosition++;

    if($this->offsetPosition < $this->lastCount) {
      // Still acting in current row group
      // We already have the data in the buffer
    } else {
      if($this->lastCount >= 0 && ($this->totalReadCount < $this->count())) {
        do {
          $this->read();
        } while($this->lastCount === 0);
      }
    }
  }

  /**
   * @inheritDoc
   */
  public function key()
  {
    return $this->position;
  }

  /**
   * @inheritDoc
   */
  public function valid()
  {
    return $this->offsetPosition < $this->lastCount;
  }

  /**
   * @inheritDoc
   */
  public function rewind()
  {
    $this->position = 0;
    $this->offsetPosition = 0;
    $this->currentRowGroupIndex = 0;
    $this->currentRowGroupReader = null;
    $this->currentBuffer = null;
    $this->read();
  }

  /**
   * @inheritDoc
   */
  public function count()
  {
    // NOTE: never trust this value, as it may be invalid.
    // return $this->reader->getThriftMetadata()->num_rows;
    return $this->calculatedCount;
  }
}
