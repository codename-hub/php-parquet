<?php
namespace jocoon\parquet;

use Exception;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataColumn;

use jocoon\parquet\file\ThriftFooter;
use jocoon\parquet\file\DataStreamFactory;

/**
 * [ParquetWriter description]
 */
class ParquetWriter extends ParquetActor {

  /**
   * [protected description]
   * @var ThriftFooter
   */
  protected $_footer = null;

  /**
   * [public description]
   * @var Schema
   */
  public $schema;

  /**
   * [$_parquetOptions description]
   * @var ParquetOptions
   */
  protected $_parquetOptions = null;

  /**
   * [protected description]
   * @var bool
   */
  protected $dataWritten = false;

  /**
   * [protected description]
   * @var ParquetRowGroupWriter[]
   */
  protected $openedWriters = [];

  /**
   * [public description]
   * @var int
   */
  public $compressionMethod = CompressionMethod::Gzip; // NOTE: internal default compression

  /**
   * [protected description]
   * @var resource
   */
  protected $baseStream;

  /**
   * [__construct description]
   * @param Schema                $schema        [description]
   * @param resource|int          $output        [description]
   * @param ParquetOptions|null   $formatOptions [description]
   * @param bool $append          [description]
   */
  public function __construct(Schema $schema, $output, ?ParquetOptions $formatOptions = null, bool $append = false)
  {
    DataStreamFactory::registerStreamWrappers();

    $this->baseStream = $output;
    // NOTE: due to GC, we have to leave the stream open
    $gapStream = GapStreamWrapper::createWrappedStream($output, 'r+', null, true);
    parent::__construct($gapStream);

    if($output === null) {
      throw new Exception('naaah');
    }

    // if (!output.CanWrite) throw new ArgumentException("stream is not writeable", nameof(output));
    $this->schema = $schema; //  ?? throw new ArgumentNullException(nameof(schema));
    $this->_parquetOptions = $formatOptions ?? new ParquetOptions();

    $this->PrepareFile($append);
  }

  /**
   * [PrepareFile description]
   * @param bool $append [description]
   */
  protected function PrepareFile(bool $append)
  {
    if ($append)
    {
      // TODO: if (!Stream.CanSeek) throw new IOException("destination stream must be seekable for append operations.");

      $this->ValidateFile();

      $fileMeta = $this->ReadMetadata();
      $this->_footer = new ThriftFooter($fileMeta);

      $this->ValidateSchemasCompatible($this->_footer, $this->schema);

      $this->GoBeforeFooter();
    }
    else
    {
      if ($this->_footer == null)
      {
        $this->_footer = ThriftFooter::createFromSchema($this->schema, 0); // TODO: don't forget to set the total row count at the end

        //file starts with magic
        $this->WriteMagic();
      }
      else
      {
        $this->ValidateSchemasCompatible($this->_footer, $this->schema);

        // TODO: $this->_footer->Add(0 /* todo: don't forget to set the total row count at the end!!! */);
        $this->_footer->add(0 /* todo: don't forget to set the total row count at the end!!! */);
      }
    }
  }

  /**
   * [ValidateSchemasCompatible description]
   * @param ThriftFooter $footer [description]
   * @param Schema       $schema [description]
   */
  private function ValidateSchemasCompatible(ThriftFooter $footer, Schema $schema): void
  {
    $existingSchema = $footer->CreateModelSchema($this->_parquetOptions);

    if (!$schema->Equals($existingSchema))
    {
      $reason = $schema->GetNotEqualsMessage($existingSchema, "appending", "existing");
      throw new ParquetException("passed schema does not match existing file schema, reason: {reason}");
    }
  }

  /**
   * [CreateRowGroup description]
   * @return ParquetRowGroupWriter [description]
   */
  public function CreateRowGroup(): ParquetRowGroupWriter
  {
    $this->dataWritten = true;

    $writer = new ParquetRowGroupWriter($this->schema, $this->_fileStream, $this->ThriftStream, $this->_footer, $this->compressionMethod, $this->_parquetOptions);

    $this->openedWriters[] = $writer;

    return $writer;
  }

  /**
   * [WriteMagic description]
   */
  protected function WriteMagic() : void
  {
    fwrite($this->_fileStream, static::MagicBytes);
    // Stream.Write(MagicBytes, 0, MagicBytes.Length);
  }

  /**
   * [finish description]
   */
  public function finish() : void {

    if($this->dataWritten) {
      $sum = 0;
      foreach($this->openedWriters as $rgWriter) {
        // finish RG Writer here? (originally, at disposal. But this is quirky.)
        $rgWriter->finish();
        $sum += $rgWriter->getRowCount() ?? 0;
      }

      // update row count (on append add row count to existing metadata)
      $this->_footer->add($sum);
    }

    //
    // Disposal of opened writers
    //
    // $this->openedWriters = [];
    // foreach($this->openedWriters as &$rgWriter) {
    //   echo("test kill writer");
    //   $rgWriter = null;
    // }

    $size = $this->_footer->write($this->ThriftStream);

    // metadata size ?
    // $this->Writer->setPosition(ftell($this->_fileStream));
    // $this->Writer->insertInt((int)$size);
    fwrite($this->_fileStream, pack('l', (int)$size)); // 4 bytes

    $this->WriteMagic(); // 4 bytes

    fflush($this->_fileStream);
  }
}
