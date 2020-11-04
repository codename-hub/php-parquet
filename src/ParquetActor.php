<?php
namespace jocoon\parquet;

use Exception;

use jocoon\parquet\adapter\BinaryReader;

use jocoon\parquet\exception\ArgumentNullException;

use jocoon\parquet\file\ThriftStream;

use jocoon\parquet\format\FileMetaData;

/**
 * [ParquetActor description]
 */
class ParquetActor {

  /**
   * [MagicString description]
   * @var string
   */
  const MagicString = "PAR1";

  /**
   * [MagicBytes description]
   * @var string
   */
  const MagicBytes = "PAR1";

  /**
   * [protected description]
   * @var resource
   */
  protected $_fileStream;

  /**
   * [private description]
   * @var BinaryReader
   */
  protected $Reader;

  /**
   * [private description]
   * @var \jocoon\parquet\adapter\BinaryWriter
   */
  protected $Writer;

  /**
   * [private description]
   * @var ThriftStream
   */
  protected $ThriftStream;

  /**
   * @param mixed $_fileStream
   */
  protected function __construct($_fileStream)
  {
    if($_fileStream === null) {
      throw new ArgumentNullException('No stream');
    }

    if(!is_resource($_fileStream)) {
      // not a resource, cannot open?
      throw new Exception('Not a stream');
    }

    $this->_fileStream = $_fileStream;

    //
    // NOTE: Readers/writers should use little endian, by default
    //
    $this->Reader = \jocoon\parquet\adapter\BinaryReader::createInstance($this->_fileStream);
    $this->Writer = \jocoon\parquet\adapter\BinaryWriter::createInstance($this->_fileStream);

    $this->ThriftStream = new ThriftStream($this->_fileStream);
  }

  /**
   * [ValidateFile description]
   */
  protected function ValidateFile()
  {
    // _fileStream.Seek(0, SeekOrigin.Begin);
    fseek($this->_fileStream, 0, SEEK_SET);

    $shead = fread($this->_fileStream, 4);
    // char[] head = Reader.ReadChars(4);
    // string shead = new string(head);

    // _fileStream.Seek(-4, SeekOrigin.End);
    fseek($this->_fileStream, -4, SEEK_END);

    // char[] tail = Reader.ReadChars(4);
    // string stail = new string(tail);
    $stail = fread($this->_fileStream, 4);

    // if (shead != MagicString)
    //    throw new IOException($"not a Parquet file(head is '{shead}')");
    if($shead !== static::MagicString) {
      throw new \Exception("not a Parquet file(head is '{$shead}')");
    }

    // if (stail != MagicString)
    //    throw new IOException($"not a Parquet file(head is '{stail}')");
    if($stail !== static::MagicString) {
      throw new \Exception("not a Parquet file(tail is '{$stail}')");
    }
  }

  /**
   * [ReadMetadata description]
   * @return FileMetaData [description]
   */
  protected function ReadMetadata() : FileMetaData
  {
     $this->GoBeforeFooter();

     return $this->ThriftStream->Read(FileMetaData::class);
  }

  /**
   * [GoToBeginning description]
   */
  protected function GoToBeginning(): void
  {
     fseek($this->_fileStream, 0, SEEK_SET);
  }

  /**
   * [GoToEnd description]
   */
  protected function GoToEnd(): void
  {
     fseek($this->_fileStream, 0, SEEK_END);
  }

  /**
   * [GoBeforeFooter description]
   */
  protected function GoBeforeFooter(): void
  {
     // go to -4 bytes (PAR1) -4 bytes (footer length number)
     fseek($this->_fileStream, -8, SEEK_END);

     $footerLength = $this->Reader->readInt32();

     //set just before footer starts
     fseek($this->_fileStream, -8 - $footerLength, SEEK_END);
  }

}
