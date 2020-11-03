<?php
namespace jocoon\parquet;

use Exception;

use jocoon\parquet\adapter\BinaryReader;

use jocoon\parquet\exception\ArgumentNullException;

use jocoon\parquet\file\ThriftStream;

use jocoon\parquet\format\FileMetaData;

class ParquetActor {

  const MagicString = "PAR1";

  const MagicBytes = "PAR1";

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
    $this->Reader = \jocoon\parquet\adapter\BinaryReader::createInstance($this->_fileStream); // new \jocoon\parquet\adapter\BinaryReader($this->_fileStream);
    //
    // NOTE: Nelexa's ResourceBuffer is, by default, BIG ENDIAN.
    //
    $this->Writer = \jocoon\parquet\adapter\BinaryWriter::createInstance($this->_fileStream); // do we always need this? TODO: writer on-demand (NOTE ResourceBuffer is resetting position at init)
    // $this->Writer->setOrder(\jocoon\parquet\adapter\BinaryWriter::LITTLE_ENDIAN); // enforce little endian

    $this->ThriftStream = new ThriftStream($this->_fileStream);
  }

  // /**
  //  * [getWriter description]
  //  * @return \jocoon\parquet\adapter\BinaryWriter [description]
  //  */
  // public function getWriter(): \jocoon\parquet\adapter\BinaryWriter {
  //   if($this->Writer) {
  //     return $this->Writer;
  //   } else {
  //     $pos = ftell($this->_fileStream);
  //     $this->Writer = new \jocoon\parquet\adapter\BinaryWriter($this->_fileStream);
  //     $this->Writer->setPosition($pos);
  //     return $this->Writer;
  //   }
  //   // return $this->Writer ?? $this->Writer = new \jocoon\parquet\adapter\BinaryWriter($this->_fileStream); // do we always need this?
  // }

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

  protected function ReadMetadata() : FileMetaData
  {
     $this->GoBeforeFooter();

     // return ThriftStream.Read<Thrift.FileMetaData>();
     return $this->ThriftStream->Read(FileMetaData::class);
  }

  protected function GoToBeginning()
  {
     // _fileStream.Seek(0, SeekOrigin.Begin);
     fseek($this->_fileStream, 0, SEEK_SET);
  }

  protected function GoToEnd()
  {
     // _fileStream.Seek(0, SeekOrigin.End);
     fseek($this->_fileStream, 0, SEEK_END);
  }

  protected function GoBeforeFooter()
  {
     //go to -4 bytes (PAR1) -4 bytes (footer length number)
     // _fileStream.Seek(-8, SeekOrigin.End);
     fseek($this->_fileStream, -8, SEEK_END);

     // int footerLength = Reader.ReadInt32();
     $footerLength = $this->Reader->readInt32();

     //set just before footer starts
     // _fileStream.Seek(-8 - footerLength, SeekOrigin.End);
     fseek($this->_fileStream, -8 - $footerLength, SEEK_END);
  }

}
