<?php
namespace codename\parquet\helper;

use codename\parquet\ParquetWriter;
use codename\parquet\ParquetOptions;
use codename\parquet\data\Schema;

/**
 * Preliminary helper
 * to write-out arbitrary php array data
 * to parquet columns
 * while maintaining groups, pages and stuff.
 */
class ParquetDataWriter
{
  /**
   * preferred max. row group size
   * @var int
   */
  protected $rowGroupSize = 5000;

  // /**
  //  * preferred max. data page size
  //  * @var int|null
  //  */
  // protected $dataPageSize = null;

  /**
   * [protected description]
   * @var ParquetWriter
   */
  protected $writerInstance;

  /**
   * [protected description]
   * @var Schema
   */
  protected $schema;

  /**
   * [__construct description]
   * @param resource              $handle   [description]
   * @param Schema                $schema   [description]
   * @param ParquetOptions|null   $options  [description]
   */
  public function __construct($handle, Schema $schema, ?ParquetOptions $options = null)
  {
    $append = false; // TODO: detect already written data and change to true (append mode)
    $this->schema = $schema;
    $this->writerInstance = new ParquetWriter($schema, $handle, $options, $append);
  }

  /**
   * [setRowGroupSize description]
   * @param int $count  [description]
   */
  public function setRowGroupSize(int $count): void {
    if($count > 0) {
      $this->rowGroupSize = $count;
    } else {
      throw new \Exception('Invalid row group size specified');
    }
  }

  /**
   * Buffered datasets/records
   * @var array[]
   */
  protected $bufferedDatasets = [];

  /**
   * Separate counting variable to avoid consecutive calls to count()
   * @var int
   */
  protected $currentBufferedCount = 0;

  /**
   * Put a single dataset into writing buffer
   * @param array $dataset  [description]
   */
  public function put(array $dataset): void {
    $this->bufferedDatasets[] = $dataset;
    ++$this->currentBufferedCount;
    $this->handleRowGroupBuffer();
  }

  /**
   * Put multiple datasets at once
   * @param array $batch  [description]
   */
  public function putBatch(array $batch): void {
    foreach($batch as $dataset) {
      $this->bufferedDatasets[] = $dataset;
      ++$this->currentBufferedCount;
    }
    $this->handleRowGroupBuffer();
  }

  /**
   * Handles buffering and write-out
   * @param bool $override  [force write-out]
   */
  protected function handleRowGroupBuffer(bool $override = false): void {
    do {
      $more = $this->handleRowGroupBufferInternal($override);
    } while($more);
  }

  /**
   * [handleRowGroupBufferInternal description]
   * @param  bool $override    [description]
   * @return bool              [remaining RG buffer data available]
   */
  protected function handleRowGroupBufferInternal(bool $override = false): bool {

    // Avoid writing empty row groups
    if($this->currentBufferedCount === 0) {
      return false;
    }

    // We either reached desired row group size
    // or we're finishing and just writing out leftovers
    if($this->currentBufferedCount >= $this->rowGroupSize || $override) {
      // pick buffer
      $data = array_splice($this->bufferedDatasets, 0, $this->rowGroupSize);

      // refresh buffered count
      $this->currentBufferedCount = count($this->bufferedDatasets);

      // convert to data columns
      $dataColumnsConverter = new ArrayToDataColumnsConverter($this->schema, $data);
      $columns = $dataColumnsConverter->toDataColumns();

      // writeout row group batch
      $rowGroupWriter = $this->writerInstance->CreateRowGroup();
      foreach($columns as $col) {
        $rowGroupWriter->WriteColumn($col);
      }
      $rowGroupWriter->finish();
    }

    // If in override (forced-write-out) mode
    // we just have to check whether there's more data left.
    if($override) {
      return $this->currentBufferedCount > 0;
    } else {
      // Return whether we have at least the rowGroupSize count of elements
      // So we can write another RG.
      return $this->currentBufferedCount >= $this->rowGroupSize;
    }

  }

  /**
   * Finish writing
   */
  public function finish(): void {
    // Final call, write out any leftovers in buffer.
    $this->handleRowGroupBuffer(true);
    $this->writerInstance->finish();
  }
}
