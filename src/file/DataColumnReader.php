<?php
namespace jocoon\parquet\file;

use jocoon\parquet\ParquetOptions;

use jocoon\parquet\adapter\BinaryReader;

use jocoon\parquet\data\DataType;
use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataColumn;
use jocoon\parquet\data\DataTypeFactory;
use jocoon\parquet\data\DataColumnStatistics;
use jocoon\parquet\data\DataTypeHandlerInterface;

use jocoon\parquet\format\Encoding;
use jocoon\parquet\format\PageType;
use jocoon\parquet\format\PageHeader;
use jocoon\parquet\format\ColumnChunk;
use jocoon\parquet\format\SchemaElement;

use jocoon\parquet\values\RunLengthBitPackingHybridValuesReader;

class DataColumnReader
{
  /**
   * [private description]
   * @var DataField
   */
  private $dataField;

  /**
   * [private description]
   * @var resource
   */
  private $inputStream;

  /**
   * [private description]
   * @var ColumnChunk
   */
  private $thriftColumnChunk;

  /**
   * [private description]
   * @var SchemaElement
   */
  private $thriftSchemaElement;

  /**
   * [private description]
   * @var ThriftFooter
   */
  private $footer;

  /**
   * [private description]
   * @var ParquetOptions
   */
  private $parquetOptions;

  /**
   * [private description]
   * @var ThriftStream
   */
  private $thriftStream;

  /**
   * [private description]
   * @var int
   */
  private $maxRepetitionLevel;

  /**
   * [private description]
   * @var int
   */
  private $maxDefinitionLevel;

  // private IDataTypeHandler _dataTypeHandler;

  /**
   * @param DataField      $dataField
   * @param resource       $inputStream
   * @param ColumnChunk    $thriftColumnChunk
   * @param ThriftFooter   $footer
   * @param ParquetOptions $parquetOptions
   */
  public function __construct(
    DataField $dataField,
    $inputStream,
    ColumnChunk $thriftColumnChunk,
    ThriftFooter $footer,
    ParquetOptions $parquetOptions
  ) {
    $this->dataField = $dataField;
    $this->inputStream = $inputStream;
    $this->thriftColumnChunk = $thriftColumnChunk;
    $this->footer = $footer;
    $this->parquetOptions = $parquetOptions;

    $this->thriftStream = new ThriftStream($inputStream);
    $mrl = 0;
    $mdl = 0;
    // _footer.GetLevels(_thriftColumnChunk, out int mrl, out int mdl);
    $this->footer->getLevels($this->thriftColumnChunk, $mrl, $mdl);
    //      _dataField.MaxRepetitionLevel = mrl;
    //      _dataField.MaxDefinitionLevel = mdl;
    //      _maxRepetitionLevel = mrl;
    //      _maxDefinitionLevel = mdl;
    $this->dataField->maxRepetitionLevel = $mrl;
    $this->dataField->maxDefinitionLevel = $mdl;
    $this->maxRepetitionLevel = $mrl;
    $this->maxDefinitionLevel = $mdl;


    //      _thriftSchemaElement = _footer.GetSchemaElement(_thriftColumnChunk);
    $this->thriftSchemaElement = $this->footer->getSchemaElement($this->thriftColumnChunk);

    //      _dataTypeHandler = DataTypeFactory.Match(_thriftSchemaElement, _parquetOptions);

    $this->dataTypeHandler = DataTypeFactory::matchSchemaElement($this->thriftSchemaElement, $parquetOptions);
  }

  /**
   * [protected description]
   * @var DataTypeHandlerInterface
   */
  protected $dataTypeHandler;

  /**
   * [getFileOffset description]
   * @return int [description]
   */
  protected function getFileOffset() : int {
    return min(array_filter([
      $this->thriftColumnChunk->meta_data->dictionary_page_offset,
      $this->thriftColumnChunk->meta_data->data_page_offset
    ], function($v) { return $v != 0; }));
  }

  /**
   * [readDataPage description]
   * @param PageHeader    $ph        [description]
   * @param ColumnRawData $cd        [description]
   * @param int           $maxValues [description]
   */
  protected function readDataPage(PageHeader $ph, ColumnRawData $cd, int $maxValues) : void {

    $bytes = $this->readPageDataByPageHeader($ph);

    $reader = \jocoon\parquet\adapter\BinaryReader::createInstance($bytes);

    if($this->maxRepetitionLevel > 0) {
      //todo: use rented buffers, but be aware that rented length can be more than requested so underlying logic relying on array length must be fixed too.
      if($cd->repetitions === null) { // TODO/QUESTION: may they even be null?
        $cd->repetitions = array_fill(0, $cd->maxCount, null);
      }

      $cd->repetitionsOffset += $this->readLevels($reader, $this->maxRepetitionLevel, $cd->repetitions, $cd->repetitionsOffset, $ph->data_page_header->num_values);
    }

    if($this->maxDefinitionLevel > 0) {
      if($cd->definitions === null) {
        $cd->definitions = array_fill(0, $cd->maxCount, null);
      }

      $cd->definitionsOffset += $this->readLevels($reader, $this->maxDefinitionLevel, $cd->definitions, $cd->definitionsOffset, $ph->data_page_header->num_values);
    }

    if($ph->data_page_header === null) {
      throw new \Exception('file corrupt, data page header missing');
    }

    //
    // NOTE:
    // The Possible Fix from https://github.com/aloneguid/parquet-dotnet/commit/a8bfeef068ed7b29bad8a4150ed8e50362fb1720
    // is not suitable for this case, as it assumes the existence of Statistics (which are, in fact, optional).
    //
    // Original comment:
    //
    //    if statistics are defined, use null count to determine the exact number of items we should read
    //    however, I don't know if all parquet files with null values have stats defined. Maybe a better solution would
    //    be using a count of defined values (from reading definitions?)
    //
    // This is in fact based on a wrong assumption.
    // In the following passage, we're determining null count by checking definition levels.
    //

    $nullCount = $ph->data_page_header->statistics->null_count ?? null;

    //
    // Statistics' null_count is an optional field
    // if we have no data (null), we have to make sure
    // to determine the count of null values in the current data page
    //
    if($nullCount === null && $cd->definitions !== null) {
      //
      // We're counting all maxDefinitionLevel entries (non-nulls)
      // and subtract them from our known num_values (which include nulls)
      //
      // NOTE: we are only comparing the definition levels within current page bounds
      // $cd->definitionsOffset has already been incremented at this point, so we assume the initial state
      // start index: definitionsOffset - num_values
      // end index:   definitionsOffset
      //
      $definitionCount = 0;
      for ($i = $cd->definitionsOffset - $ph->data_page_header->num_values; $i < $cd->definitionsOffset; $i++) {
        if($cd->definitions[$i] === $this->maxDefinitionLevel) {
          $definitionCount++;
        }
      }

      $nullCount = $ph->data_page_header->num_values - $definitionCount;
    }

    $maxReadCount = $ph->data_page_header->num_values - (int)($nullCount ?? 0);
    $this->readColumn($reader, $ph->data_page_header->encoding, $maxValues, $maxReadCount, $cd);
  }

  /**
   * [readLevels description]
   * @param  BinaryReader $reader   [description]
   * @param  int          $maxLevel [description]
   * @param  array        &$dest    [description]
   * @param  int|null     $offset   [description]
   * @param  int|null     $pageSize [description]
   * @return int                    [description]
   */
  protected function readLevels(BinaryReader $reader, int $maxLevel, array &$dest, ?int $offset, ?int $pageSize) : int {
    $bitWidth = static::getBitWidth($maxLevel);
    return RunLengthBitPackingHybridValuesReader::ReadRleBitpackedHybrid($reader, $bitWidth, 0, $dest, $offset, $pageSize);
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

  protected function readColumn(BinaryReader $reader, int $encoding, int $totalValues, int $maxReadCount, ColumnRawData $cd)
  {
     //dictionary encoding uses RLE to encode data

     // print_r([
     //   'action' => 'readColumn',
     //   'dataFieldName' => $this->dataField->name,
     //   'totalValues' => $totalValues,
     // ]);

     if ($cd->values === null)
     {
        $cd->values = array_fill(0, $totalValues, null);
        // $cd->values = $this->dataTypeHandler->getArray((int)$totalValues, false, false);
     }

     if($cd->valuesOffset === null) {
       $cd->valuesOffset = 0;
     }

     switch ($encoding)
     {
        case Encoding::PLAIN:
           $cd->valuesOffset += $this->dataTypeHandler->read($reader, $this->thriftSchemaElement, $cd->values, $cd->valuesOffset);
           break;

        case Encoding::RLE:
          if ($cd->indexes === null) {
            // QUESTION: should we pre-fill the array?
            $cd->indexes = array_fill(0, $totalValues, 0);
          }
          $indexCount = RunLengthBitPackingHybridValuesReader::Read($reader, $this->thriftSchemaElement->type_length, $cd->indexes, 0, $maxReadCount);
          $this->dataTypeHandler->mergeDictionary($cd->dictionary, $cd->indexes, $cd->values, $cd->valuesOffset, $indexCount);
          $cd->valuesOffset += $indexCount;
          break;

        case Encoding::PLAIN_DICTIONARY:
          if($cd->indexes === null) {
            // QUESTION: should we pre-fill the array?
            $cd->indexes = array_fill(0, $totalValues, null);
          }
          $indexCount = static::readPlainDictionary($reader, $maxReadCount, $cd->indexes, 0);
          $this->dataTypeHandler->mergeDictionary($cd->dictionary, $cd->indexes, $cd->values, $cd->valuesOffset, $indexCount);

          $cd->valuesOffset += $indexCount;
          break;

        default:
           throw new \Exception("encoding {$encoding} is not supported.");
     }
  }

  /**
   * [ReadPlainDictionary description]
   * @param  BinaryReader $reader       [description]
   * @param  int          $maxReadCount [description]
   * @param  int[]        &$dest         [description]
   * @param  int          $offset       [description]
   * @return int                        [description]
   */
  protected static function ReadPlainDictionary(BinaryReader $reader, int $maxReadCount, array &$dest, int $offset): int
  {
    $start = $offset;
    $bitWidth = ord($reader->readBytes(1)); // read byte?

    //
    //  parquet-dotnet PR #96 port
    //  Fix reading of plain dictionary with zero length
    //
    $length = static::GetRemainingLength($reader);

    //when bit width is zero reader must stop and just repeat zero maxValue number of times
    if ($bitWidth === 0 || $length === 0)
    {
      for ($i = 0; $i < $maxReadCount; $i++)
      {
        $dest[$offset++] = 0;
      }
    }
    else
    {
      //
      // parquet-dotnet PR #95 port
      // Empty page handling
      //
      if($length !== 0) {
        $offset += RunLengthBitPackingHybridValuesReader::ReadRleBitpackedHybrid($reader, $bitWidth, $length, $dest, $offset, $maxReadCount);
      }
    }

    return $offset - $start;
  }

  /**
   * [GetRemainingLength description]
   * @param  BinaryReader $reader [description]
   * @return int                  [description]
   */
  private static function GetRemainingLength(BinaryReader $reader): int
  {
    // return (int)(fstat($reader->getInputHandle())['size'] - ftell($reader->getInputHandle()));
    return (int)($reader->getEofPosition() - $reader->getPosition());
  }

  /**
   * [read description]
   * @return DataColumn [description]
   */
  public function read() : DataColumn {

    $fileOffset = $this->getFileOffset();
    $maxValues = $this->thriftColumnChunk->meta_data->num_values;

    // print_r("fileOffset=".$fileOffset);
    // print_r([
    //   '$fileOffset' => $fileOffset,
    //   'ftell' => ftell($this->inputStream),
    //   'columnChunkFileOffset' => $this->thriftColumnChunk->file_offset
    // ]);

    fseek($this->inputStream, $fileOffset, SEEK_SET);

    // print_r([
    //   '$fileOffset' => $fileOffset,
    //   'ftell' => ftell($this->inputStream),
    //   'columnChunkFileOffset' => $this->thriftColumnChunk->file_offset
    // ]);


    $colData = new ColumnRawData();
    $colData->maxCount = $this->thriftColumnChunk->meta_data->num_values;

    // $ph = new PageHeader();
    // NOTE: we have to read it at this point!
    // $ph = $ph->Read($this->thriftStream);
    // $this->thriftStream->ReadByInstance($ph);
    $ph = $this->thriftStream->Read(PageHeader::class);

    $dictionary = [];
    $dictionaryOffset = 0;

    if($this->TryReadDictionaryPage($ph, $dictionary, $dictionaryOffset)) {
      $ph = $this->thriftStream->Read(PageHeader::class);

      // echo("DictionaryPage!");
      // var_dump($ph);
      // $ph = $ph->Read($this->thriftStream);
    } else {
      // JUST DEBUG
      // echo(chr(10).chr(10).chr(10)."*** DBG ***".chr(10).chr(10).chr(10));
      // print_r($ph);
      // echo(chr(10).chr(10).chr(10)."*** DBG ***".chr(10).chr(10).chr(10));
      // die();
    }

    $colData->dictionary = $dictionary;
    $colData->dictionaryOffset = $dictionaryOffset;
    // if (TryReadDictionaryPage(ph, out colData.dictionary, out colData.dictionaryOffset))
    //      {
    //         ph = _thriftStream.Read<Thrift.PageHeader>();
    //      }
    // ->>> $ph->read($this->thriftStream);

    // print_r($colData);
    // print_r($ph);

    $pagesRead = 0;

    while(true) {

      // echo(chr(10)."readDataPage".chr(10));
      $this->readDataPage($ph, $colData, $maxValues);
      $pagesRead++;

      // int totalCount = Math.Max(
      //    (colData.values == null ? 0 : colData.valuesOffset),
      //    (colData.definitions == null ? 0 : colData.definitionsOffset));

      $totalCount = max(
        ($colData->values === null ? 0 : $colData->valuesOffset),
        ($colData->definitions === null ? 0 : $colData->definitionsOffset)
      );

      // if (totalCount >= maxValues) break; //limit reached
      if($totalCount >= $maxValues) {
        // var_dump(
        //   [
        //     '$colData->values === null' => $colData->values === null,
        //     '$colData->definitions === null' => $colData->definitions === null,
        //     '$colData->valuesOffset' => $colData->valuesOffset,
        //     '$colData->definitionsOffset' => $colData->definitionsOffset,
        //   ]
        // );
        // echo("Breaking out due to $totalCount >= $maxValues");

        break;
      }

      // ph = _thriftStream.Read<Thrift.PageHeader>();
      // $ph->read($this->thriftStream);

      // $res = $this->thriftStream->ReadByInstance($ph);
      $ph = $this->thriftStream->Read(PageHeader::class);

      // $ph2 = $this->thriftStream->Read(PageHeader::class);

      // if (ph.Type != Thrift.PageType.DATA_PAGE) break;
      if($ph->type != PageType::DATA_PAGE) {
        break; // V2 support?
      }

    }

    // all the data is available here!

    // return new DataColumn(
    //   _dataField, colData.values,
    //   colData.definitions, _maxDefinitionLevel,
    //   colData.repetitions, _maxRepetitionLevel,
    //   colData.dictionary,
    //   colData.indexes);

    $finalColumn = DataColumn::DataColumnExtended(
      $this->dataField, $colData->values,
      $colData->definitions, $this->maxDefinitionLevel,
      $colData->repetitions, $this->maxRepetitionLevel,
      $colData->dictionary,
      $colData->indexes
    );

    // DEBUG
    // print_r($this->thriftColumnChunk->meta_data);

    if($this->thriftColumnChunk->meta_data->statistics) {
      $finalColumn->statistics = new DataColumnStatistics(
        $this->thriftColumnChunk->meta_data->statistics->null_count,
        $this->thriftColumnChunk->meta_data->statistics->distinct_count,
        $this->dataTypeHandler->plainDecode($this->thriftSchemaElement, $this->thriftColumnChunk->meta_data->statistics->min_value),
        $this->dataTypeHandler->plainDecode($this->thriftSchemaElement, $this->thriftColumnChunk->meta_data->statistics->max_value)
      );
    }

    return $finalColumn;

  }

  /**
   * [TryReadDictionaryPage description]
   * @param  PageHeader   $ph               [description]
   * @param  array|null   &$dictionary       [description]
   * @param  int|null     &$dictionaryOffset [description]
   * @return bool                         [description]
   */
  private function TryReadDictionaryPage(PageHeader $ph, ?array &$dictionary, ?int &$dictionaryOffset) : bool
  {
    if ($ph->type != PageType::DICTIONARY_PAGE)
    {
      $dictionary = null;
      $dictionaryOffset = 0;
      return false;
    }

    //Dictionary page format: the entries in the dictionary - in dictionary order - using the plain encoding.


    // using (BytesOwner bytes = ReadPageData(ph))
    // {
    $bytes = $this->readPageDataByPageHeader($ph);
      //todo: this is ugly, but will be removed once other parts are migrated to System.Memory
      // using (var ms = new MemoryStream(bytes.Memory.ToArray()))
      // {
      // $ms = $bytes;

        // using (var dataReader = new BinaryReader(ms))
        // {
    $dataReader = \jocoon\parquet\adapter\BinaryReader::createInstance($bytes); // new \jocoon\parquet\adapter\PhpBinaryReader($ms);


        // dictionary = _dataTypeHandler.GetArray(ph.Dictionary_page_header.Num_values, false, false);


    // NOTE: we have to pre-fill the dictionary at this point.
    // Otherwise, some counts and iterators won't do their work correctly
    // We might even HAVE to implement this f*cking getArray method.
    $dictionary = array_fill(0, $ph->dictionary_page_header->num_values, null); // $this->dataTypeHandler->getArray($ph->dictionary_page_header->num_values, false, false)

        // dictionaryOffset = _dataTypeHandler.Read(dataReader, _thriftSchemaElement, dictionary, 0);
        // $dictionaryOffset = $this->dataTypeHandler->read($dataReader, $this->thriftSchemaElement, $dictionary, 0);

        // $this->thriftSchemaElement->

    $dictionaryOffset = $this->dataTypeHandler->read($dataReader, $this->thriftSchemaElement, $dictionary, 0);

        // for ($i=0; $i < $ph->dictionary_page_header->num_values; $i++) {
        //   // code...
        //
        //   // $dictionary[] = $this->thriftSchemaElement->read($dataReader);
        //   // print_r($ex);
        // }

    return true;
        // }
      // }
    // }
  }

  /**
   * Reads data according to page header
   * @param  PageHeader $pageHeader [description]
   * @return string                 [description]
   */
  protected function readPageDataByPageHeader(PageHeader $pageHeader) {
    return DataStreamFactory::ReadPageData(
      $this->inputStream,
      $this->thriftColumnChunk->meta_data->codec,
      $pageHeader->compressed_page_size, $pageHeader->uncompressed_page_size
    );
  }

}

class ColumnRawData {
  /**
   * [public description]
   * @var int
   */
  public $maxCount = 0;

  /**
   * [public description]
   * @var int[]
   */
  public $repetitions;

  /**
   * [public description]
   * @var int
   */
  public $repetitionsOffset = 0;

  /**
   * [public description]
   * @var int[]
   */
  public $definitions;

  /**
   * [public description]
   * @var int
   */
  public $definitionsOffset = 0;

  /**
   * [public description]
   * @var int[]
   */
  public $indexes;

  /**
   * [public description]
   * @var array
   */
  public $values;

  /**
   * [public description]
   * @var int
   */
  public $valuesOffset = 0;

  /**
   * [public description]
   * @var array
   */
  public $dictionary;

  /**
   * [public description]
   * @var int
   */
  public $dictionaryOffset = 0;
}
