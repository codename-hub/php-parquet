<?php
namespace codename\parquet\file;

use codename\parquet\StreamHelper;

use codename\parquet\data\DataColumn;
use codename\parquet\data\DataTypeHandlerInterface;

use codename\parquet\format\PageHeader;
use codename\parquet\format\Statistics;
use codename\parquet\format\ColumnChunk;
use codename\parquet\format\SchemaElement;
use codename\parquet\format\DataPageHeader;

use codename\parquet\values\RunLengthBitPackingHybridValuesWriter;

/**
 * [PageTag description]
 */
class PageTag
{
  /**
  * [public description]
  * @var int
  */
  public $HeaderSize;

  /**
  * [public description]
  * @var PageHeader
  */
  public $HeaderMeta;
}

class DataColumnWriter
{
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
   * @var SchemaElement
   */
  protected $schemaElement;

  /**
   * [protected description]
   * @var int
   */
  protected $compressionMethod;

  /**
   * [protected description]
   * @var int
   */
  protected $rowCount;

  /**
   * [public description]
   * @var bool
   */
  public $calculateStatistics = false;

  /**
   * Whether to write V2 Data page headers or not
   * @var bool
   */
  public $writeDataPageV2 = false;


  /**
   * [__construct description]
   * @param [type]        $stream            [description]
   * @param ThriftStream  $thriftStream      [description]
   * @param ThriftFooter  $footer            [description]
   * @param SchemaElement $schemaElement     [description]
   * @param int           $compressionMethod [description]
   * @param int           $rowCount          [description]
   */
  public function __construct(
    $stream,
    ThriftStream $thriftStream,
    ThriftFooter $footer,
    SchemaElement $schemaElement,
    int $compressionMethod,
    int $rowCount
  ) {
    $this->stream = $stream;
    $this->thriftStream = $thriftStream;
    $this->footer = $footer;
    $this->schemaElement = $schemaElement;
    $this->compressionMethod = $compressionMethod;
    $this->rowCount = $rowCount;
  }

  /**
   * [Write description]
   * @param  string[]                     $path            [description]
   * @param  DataColumn                   $column          [description]
   * @param  DataTypeHandlerInterface     $dataTypeHandler [description]
   * @return ColumnChunk                               [description]
   */
  public function Write(array $path, DataColumn $column, DataTypeHandlerInterface $dataTypeHandler): ColumnChunk
  {
    $chunk = $this->footer->CreateColumnChunk($this->compressionMethod, $this->stream, $this->schemaElement->type, $path, 0);
    $ph = $this->footer->CreateDataPage(count($column->getData()));

    $maxRepetitionLevel = 0;
    $maxDefinitionLevel = 0;

    // _footer.GetLevels(chunk, out int maxRepetitionLevel, out int maxDefinitionLevel);
    $this->footer->GetLevels($chunk, $maxRepetitionLevel, $maxDefinitionLevel);

    $pages = $this->WriteColumn($column, $this->schemaElement, $dataTypeHandler, $maxRepetitionLevel, $maxDefinitionLevel);
    // generate stats for column chunk
    $chunk->meta_data->statistics = $column->statistics->ToThriftStatistics($dataTypeHandler, $this->schemaElement);

    //this count must be set to number of all values in the column, including nulls.
    //for hierarchy/repeated columns this is a count of flattened list, including nulls.
    // chunk.Meta_data.Num_values = ph.Data_page_header.Num_values;
    $chunk->meta_data->num_values = $ph->data_page_header->num_values;
    $ph->data_page_header->statistics = $chunk->meta_data->statistics; // simply copy statistics to page header

    //the following counters must include both data size and header size
    // chunk.Meta_data.Total_compressed_size = pages.Sum(p => p.HeaderMeta.Compressed_page_size + p.HeaderSize);
    // chunk.Meta_data.Total_uncompressed_size = pages.Sum(p => p.HeaderMeta.Uncompressed_page_size + p.HeaderSize);

    $compressedSizeSum = 0;
    $uncompressedSizeSum = 0;
    foreach($pages as $page) {
      $compressedSizeSum += $page->HeaderMeta->compressed_page_size + $page->HeaderSize;
      $uncompressedSizeSum += $page->HeaderMeta->uncompressed_page_size + $page->HeaderSize;
    }

    $chunk->meta_data->total_compressed_size = $compressedSizeSum;
    $chunk->meta_data->total_uncompressed_size = $uncompressedSizeSum;

    return $chunk;
  }

  /**
   * [WriteColumn description]
   * @param  DataColumn               $column             [description]
   * @param  SchemaElement            $tse                [description]
   * @param  DataTypeHandlerInterface $dataTypeHandler    [description]
   * @param  int                      $maxRepetitionLevel [description]
   * @param  int                      $maxDefinitionLevel [description]
   * @param  Statistics               $statistics         [description]
   * @return PageTag[]                                    [description]
   */
  protected function WriteColumn(DataColumn $column,
    SchemaElement $tse,
    DataTypeHandlerInterface $dataTypeHandler,
    int $maxRepetitionLevel,
    int $maxDefinitionLevel): array
  {
    $pages = [];

    /*
    * Page header must precede actual data (compressed or not) however it contains both
    * the uncompressed and compressed data size which we don't know! This somehow limits
    * the write efficiency.
    */

    $ms = fopen('php://memory', 'r+');

    // if we're writing DataPage V2 format
    // we need a separate (uncompressed) stream for writing
    // definition and repetition levels
    $levelsV2ms = null;
    if($this->writeDataPageV2) {
      $levelsV2ms = fopen('php://memory', 'r+');
    }

    $numValues = count($column->getData());
    $dataPageHeader = $this->footer->CreateDataPage($numValues, $this->writeDataPageV2);

    if($this->writeDataPageV2) {
      $dataPageHeader->data_page_header_v2->is_compressed = ($this->compressionMethod !== \codename\parquet\CompressionMethod::None);
    }

    //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
    // using (GapStream pageStream = DataStreamFactory.CreateWriter(ms, _compressionMethod, true))
    // {

    // PageStream is a GapStream
    $pageStream = DataStreamFactory::CreateWriter($ms, $this->compressionMethod, true);

    // print_r(fstat($pageStream));

    // using (var writer = new BinaryWriter(pageStream, Encoding.UTF8, true))
    // {
    // $writer = new BinaryWriter
    $writer = \codename\parquet\adapter\BinaryWriter::createInstance($pageStream);

    // For V1 format, the writer will stay the same
    // as the stream of actual data
    // as DLs and RLs are also compressed (if applicable)
    $levelsWriter = $writer;
    if($this->writeDataPageV2) {
      // For V2, we simply create a non-wrapped writer on the raw stream
      // instead of wrapping a potentially compressed or encrypted writer around it.
      $levelsWriter = \codename\parquet\adapter\BinaryWriter::createInstance($levelsV2ms);
    }

    // $writer->setOrder(\codename\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian

    // if (column.RepetitionLevels != null)
    // {
    //   WriteLevels(writer, column.RepetitionLevels, column.RepetitionLevels.Length, maxRepetitionLevel);
    // }
    if($column->repetitionLevels !== null) {
      if($this->writeDataPageV2) {
        // V2 has different required fields
        // so we set them right here (determine count of rows by counting rl === 0 entries)
        $rowCount = 0;
        foreach($column->repetitionLevels as $rl) {
          if($rl === 0) ++$rowCount;
        }
        $dataPageHeader->data_page_header_v2->num_rows = $rowCount;
        $dataPageHeader->data_page_header_v2->repetition_levels_byte_length = $this->WriteLevelsV2($levelsWriter, $column->repetitionLevels, count($column->repetitionLevels), $maxRepetitionLevel);
      } else {
        $this->WriteLevels($levelsWriter, $column->repetitionLevels, count($column->repetitionLevels), $maxRepetitionLevel);
      }
    } else {
      if($this->writeDataPageV2) {
        // No repetitions: 1 value == 1 row
        $dataPageHeader->data_page_header_v2->num_rows = $numValues; // Only available in V2
      }
    }


    $data = $column->getData();

    if ($maxDefinitionLevel > 0)
    {
      // Definition levels might already be set
      // e.g. by ArrayToDatacolumnsConverter
      $definitionLevels = $column->definitionLevels ?? [];
      $definitionLevelsLength = 0;
      $nullCount = 0;

      $data = $column->PackDefinitions($maxDefinitionLevel, $definitionLevels, $definitionLevelsLength, $nullCount);

      //last chance to capture null count as null data is compressed now
      $column->statistics->nullCount = $nullCount;

      try
      {
        if($this->writeDataPageV2) {
          // V2 has different required fields
          // so we set them right here
          $dataPageHeader->data_page_header_v2->num_nulls = $nullCount;
          $dataPageHeader->data_page_header_v2->definition_levels_byte_length = $this->WriteLevelsV2($levelsWriter, $definitionLevels, $definitionLevelsLength, $maxDefinitionLevel);
        } else {
          $this->WriteLevels($levelsWriter, $definitionLevels, $definitionLevelsLength, $maxDefinitionLevel);
        }
      }
      finally
      {
        if ($definitionLevels !== null)
        {
          // clear 'buffer'
          $definitionLevels = [];
          // ArrayPool<int>.Shared.Return(definitionLevels);
        }
      }
    }
    else
    {
      //no definitions means no nulls
      $column->statistics->nullCount = 0;
      if($this->writeDataPageV2) {
        $dataPageHeader->data_page_header_v2->num_nulls = 0; // do we have to set it?
      }
    }

    // TODO: enable or disable statistics generation
    if($this->calculateStatistics) {
      $dataTypeHandler->Write($tse, $writer, $data, $column->statistics);
    } else {
      $dataTypeHandler->Write($tse, $writer, $data);
    }

    // $writer->close();
    // $writer->(); // Flush();

    fflush($pageStream);

    StreamHelper::MarkWriteFinished($pageStream);

    // pageStream.Flush();   //extremely important to flush the stream as some compression algorithms don't finish writing
    // pageStream.MarkWriteFinished();
    // dataPageHeader.Uncompressed_page_size = (int)pageStream.Position;
    $dataPageHeader->uncompressed_page_size = (int)ftell($pageStream);

    if($this->writeDataPageV2) {
      // for V2 we simply need to add the bytes fritten to levelsV2ms
      $dataPageHeader->uncompressed_page_size += $dataPageHeader->data_page_header_v2->repetition_levels_byte_length + $dataPageHeader->data_page_header_v2->definition_levels_byte_length;
    }

    //
    // NOTE: the GzipStreamWrapper writes out/compresses data on stream close.
    //
    fclose($pageStream); // should we really close this here?

    $dataPageHeader->compressed_page_size = (int)ftell($ms);

    if($this->writeDataPageV2) {
      // for V2 we simply need to add the separately written bytes of DLs and RLs
      $dataPageHeader->compressed_page_size += $dataPageHeader->data_page_header_v2->repetition_levels_byte_length + $dataPageHeader->data_page_header_v2->definition_levels_byte_length;
    }

    // apply statistics to respective DP header object
    if($this->writeDataPageV2) {
      $dataPageHeader->data_page_header_v2->statistics = $column->statistics->ToThriftStatistics($dataTypeHandler, $this->schemaElement);
    } else {
      $dataPageHeader->data_page_header->statistics = $column->statistics->ToThriftStatistics($dataTypeHandler, $this->schemaElement);
    }

    //write the header in
    $headerSize = $this->thriftStream->Write(PageHeader::class, $dataPageHeader);

    if($this->writeDataPageV2) {
      // Rewind the levelsV2 stream for copying
      fseek($levelsV2ms, 0);

      // write the separately written levels
      $levelBytes = stream_copy_to_stream($levelsV2ms, $this->stream);
    }

    // ms.CopyTo(_stream);

    // Rewind the data stream for copying
    fseek($ms, 0);
    $copiedBytes = stream_copy_to_stream($ms, $this->stream);

    // $stats = new Statistics(['distinct_count' => $statistics->distinct_count]);
    // {
    //    Distinct_count = statistics.Distinct_count
    // };
    // $dataPageHeader->data_page_header->statistics = $stats;

    // var dataTag = new PageTag
    // {
    //    HeaderMeta = dataPageHeader,
    //    HeaderSize = headerSize
    // };
    $dataTag = new PageTag();
    $dataTag->HeaderMeta = $dataPageHeader;
    $dataTag->HeaderSize = $headerSize;

    $pages[] = $dataTag;
    // pages.Add(dataTag);

    // FFlush MS here?
    fflush($ms);
    fclose($ms);

    return $pages;
  }

  /**
   * [getBitWidth description]
   * @param  int $value [description]
   * @return int        [description]
   */
  public static function getBitWidth(int $value) : int
  {
     for ($i = 0; $i < 64; $i++)
     {
        if ($value === 0) return $i;
        $value >>= 1;
     }
     return 1;
  }

  /**
   * [WriteLevels description]
   * @param \codename\parquet\adapter\BinaryWriter $writer   [description]
   * @param int[]                      $levels   [description]
   * @param int                        $count    [description]
   * @param int                        $maxLevel [description]
   */
  protected function WriteLevels(\codename\parquet\adapter\BinaryWriter $writer, array $levels, int $count, int $maxLevel): void
  {
    $bitWidth = static::GetBitWidth($maxLevel);
    RunLengthBitPackingHybridValuesWriter::WriteForwardOnly($writer, $bitWidth, $levels, $count);
  }

  /**
   * [WriteLevelsV2 description]
   * @param \codename\parquet\adapter\BinaryWriter $writer    [description]
   * @param array                              $levels    [description]
   * @param int                                $count     [description]
   * @param int                                $maxLevel  [description]
   * @return int Amount of bytes written
   */
  protected function WriteLevelsV2(\codename\parquet\adapter\BinaryWriter $writer, array $levels, int $count, int $maxLevel): int
  {
    $bitWidth = static::GetBitWidth($maxLevel);

    // we have to leave out size byte, as we don't need it due to the availability of lengths for DLs and RLs
    // otherwise this would break format spec
    return RunLengthBitPackingHybridValuesWriter::WritePureRle($writer, $bitWidth, $levels, $count);
  }

}
