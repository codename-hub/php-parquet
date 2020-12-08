<?php
namespace jocoon\parquet\file;

use jocoon\parquet\StreamHelper;

use jocoon\parquet\data\DataColumn;
use jocoon\parquet\data\DataTypeHandlerInterface;

use jocoon\parquet\format\PageHeader;
use jocoon\parquet\format\Statistics;
use jocoon\parquet\format\ColumnChunk;
use jocoon\parquet\format\SchemaElement;
use jocoon\parquet\format\DataPageHeader;

use jocoon\parquet\values\RunLengthBitPackingHybridValuesWriter;

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

    $dataPageHeader = $this->footer->CreateDataPage(count($column->getData()));

    //chain streams together so we have real streaming instead of wasting undefraggable LOH memory
    // using (GapStream pageStream = DataStreamFactory.CreateWriter(ms, _compressionMethod, true))
    // {

    // PageStream is a GapStream
    $pageStream = DataStreamFactory::CreateWriter($ms, $this->compressionMethod, true);

    // print_r(fstat($pageStream));

    // using (var writer = new BinaryWriter(pageStream, Encoding.UTF8, true))
    // {
    // $writer = new BinaryWriter
    $writer = \jocoon\parquet\adapter\BinaryWriter::createInstance($pageStream);
    // $writer->setOrder(\jocoon\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian

    // if (column.RepetitionLevels != null)
    // {
    //   WriteLevels(writer, column.RepetitionLevels, column.RepetitionLevels.Length, maxRepetitionLevel);
    // }
    if($column->repetitionLevels !== null) {
      $this->WriteLevels($writer, $column->repetitionLevels, count($column->repetitionLevels), $maxRepetitionLevel);
    }


    $data = $column->getData();

    if ($maxDefinitionLevel > 0)
    {
      $definitionLevels = [];
      $definitionLevelsLength = 0;
      $nullCount = 0;

      $data = $column->PackDefinitions($maxDefinitionLevel, $definitionLevels, $definitionLevelsLength, $nullCount);

      //last chance to capture null count as null data is compressed now
      $column->statistics->nullCount = $nullCount;

      try
      {
        $this->WriteLevels($writer, $definitionLevels, $definitionLevelsLength, $maxDefinitionLevel);
      }
      // catch(\Throwable $t) {
      //   echo("WRITE LEVELS ERROR ". $t->getMessage());
      //   print_r([
      //     'message' => $t->getMessage(),
      //     'file' => $t->getFile(),
      //     'line' => $t->getLine(),
      //     'trace' => array_map(function($te) {
      //       return [
      //         'file' => $te['file'],
      //         'line' => $te['line']
      //       ];
      //     }, $t->getTrace()),
      //   ]);
      //   // die();
      //   // print_r($t);
      // }
      finally
      {
        if ($definitionLevels !== null)
        {
          $definitionLevels = [];
          // ArrayPool<int>.Shared.Return(definitionLevels);
        }
      }
    }
    else
    {
      //no defitions means no nulls
      $column->statistics->nullCount = 0;
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

    //
    // NOTE: the GzipStreamWrapper writes out/compresses data on stream close.
    //
    fclose($pageStream); // should we really close this here?

    $dataPageHeader->compressed_page_size = (int)ftell($ms);

    //write the header in
    $dataPageHeader->data_page_header->statistics = $column->statistics->ToThriftStatistics($dataTypeHandler, $this->schemaElement);
    $headerSize = $this->thriftStream->Write(PageHeader::class, $dataPageHeader);

    // echo("DataPageHeader Written");
    // print_r($headerSize);
    // print_r($dataPageHeader);

    // ms.Position = 0;
    fseek($ms, 0);

    // ms.CopyTo(_stream);
    $copiedBytes = stream_copy_to_stream($ms, $this->stream);
    // echo("Copied bytes=".$copiedBytes.chr(10));

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
   * @param \jocoon\parquet\adapter\BinaryWriter $writer   [description]
   * @param int[]                      $levels   [description]
   * @param int                        $count    [description]
   * @param int                        $maxLevel [description]
   */
  protected function WriteLevels(\jocoon\parquet\adapter\BinaryWriter $writer, array $levels, int $count, int $maxLevel): void
  {
    // echo("WRITE LEVELS");
    $bitWidth = static::GetBitWidth($maxLevel);
    // echo("WriteLevels bitWidth=".$bitWidth." levels=".implode(",", $levels) ." count=".$count." maxLevel=".$maxLevel.chr(10));
    RunLengthBitPackingHybridValuesWriter::WriteForwardOnly($writer, $bitWidth, $levels, $count);
  }

}
