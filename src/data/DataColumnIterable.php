<?php
namespace codename\parquet\data;

use codename\parquet\file\DataColumnReader;

/**
 * DataColumn variant that implements Iterator interfaces
 * and reduces memory load to a certain level
 * as we do not have to have everything in memory at all times
 *
 * The iterator iterates over the values themselves
 * but provides currentDefinitionLevel() and currentRepetitionLevel()
 * which return the respective current DL/RL values of the most recent iteratee
 */
class DataColumnIterable extends DataColumn implements \Iterator, \Countable
{
  /**
  * [protected description]
  * @var DataColumnReader
  */
  protected $dataColumnReader;

  /**
   * Key/overall position in this column chunk
   * (across all data pages of this column)
   * @var int
   */
  protected $position = 0;

  /**
   * [__construct description]
   * @param DataField        $field
   * @param DataColumnReader $reader
   */
  public function __construct(DataField $field, DataColumnReader $reader)
  {
    $this->init($field);
    $this->field = $field;
    $this->dataColumnReader = $reader;
    $this->position = 0;
  }

  /**
   * Last read-until offset in file
   * 0 is not a real offset, but the starting point of the chunk
   * @var int
   */
  protected $fileOffset = 0;

  /**
   * Count of entries in last batch/data page
   * @var int
   */
  protected $lastCount = 0;

  /**
   * Count of entries already read
   * @var int
   */
  protected $totalReadCount = 0;

  /**
   * @inheritDoc
   */
  public function hasRepetitions(): bool
  {
    // This should be enough to track whether we need to handle repetitions
    // at some point
    return $this->field->maxRepetitionLevel > 0;
  }

  /**
   * (Tries) to read a new batch of entries
   */
  protected function read(): void {
    $this->data = [];
    $this->definitionLevels = [];
    $this->repetitionLevels = $this->repetitionLevels ? [] : null;

    //
    // NOTE: it is possible that we get a count of 0
    // but we haven't reached EOF/end of chunk
    // this means might be dealing with empty chunks/data pages (!)
    //
    $newOffset = $this->dataColumnReader->readOneDataPage(
      $this->fileOffset,
      $this->data,
      $this->definitionLevels,
      $this->repetitionLevels
    );

    // EOF/End of column chunk for this field/column
    if($newOffset < $this->fileOffset) {
      $this->lastCount = -1;
      $this->offsetPosition = 0;
      return;
    }

    // We have to unpack DLs as always.
    if($this->definitionLevels !== null) {
      $hasValueFlags = [];
      $this->data = $this->dataTypeHandler->unpackDefinitions($this->data, $this->definitionLevels, $this->field->maxDefinitionLevel, $hasValueFlags); // TODO!
      $this->hasValueFlags = $hasValueFlags;
    }

    // Set fileOffset to the last one we read up to
    $this->fileOffset = $newOffset;
    $this->lastCount = count($this->data);
    $this->totalReadCount += $this->lastCount;
    $this->offsetPosition = 0;
  }

  /**
   * @inheritDoc
   */
  public function getData(): array
  {
    throw new \Exception('getData() must not be called for a DataColumnIterable');
  }

  /**
   * @inheritDoc
   */
  public function current()
  {
    return $this->data[$this->offsetPosition];
  }

  /**
   * Returns the current definition level during iteration
   * for the current iteratee
   * @return int|null
   */
  public function currentDefinitionLevel(): ?int {
    return $this->definitionLevels[$this->offsetPosition];
  }

  /**
   * Returns the current repetition level during iteration
   * for the current iteratee
   * @return int|null
   */
  public function currentRepetitionLevel(): ?int {
    return $this->repetitionLevels[$this->offsetPosition];
  }

  /**
   * Position/index in the current batch of entries read
   * (e.g. index in our 'buffer')
   * @var int
   */
  protected $offsetPosition = 0;

  /**
   * @inheritDoc
   */
  public function next(): void
  {
    $this->position++;
    $this->offsetPosition++;

    if($this->offsetPosition < $this->lastCount) {
      // still in already-read data
      // we just incremented our offsetPosition (index in our buffer)
    } else {
      // If lastCount is -1, we reached the end.
      // we read until we read all expected values
      if($this->lastCount >= 0 && ($this->totalReadCount < $this->dataColumnReader->getThriftColumnChunk()->meta_data->num_values)) {
        // Skip empty data pages
        // lastCount === 0 is possible, but rare.
        do {
          $this->read();
        } while ($this->lastCount === 0);
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
  public function valid(): bool
  {
    return $this->offsetPosition < $this->lastCount;
  }

  /**
   * @inheritDoc
   */
  public function rewind(): void
  {
    $this->position = 0;
    $this->offsetPosition = 0;
    $this->fileOffset = 0;
    $this->totalReadCount = 0;
    $this->read();
  }

  /**
   * @inheritDoc
   */
  public function count(): int
  {
    return $this->dataColumnReader->getThriftColumnChunk()->meta_data->num_values;
  }
}
