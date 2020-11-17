<?php
namespace jocoon\parquet;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataColumn;

/**
 * [ParquetExtensions description]
 */
class ParquetExtensions
{
  /**
   * Writes a file with a single row group
   * @param [type]              $stream  [description]
   * @param Schema              $schema  [description]
   * @param DataColumn[]        $columns [description]
   * @param ParquetOptions|null $options
   */
  public static function WriteSingleRowGroupParquetFile($stream, Schema $schema, array $columns, ?ParquetOptions $options = null): void
  {
    $writer = new ParquetWriter($schema, $stream, $options);
    $writer->compressionMethod = CompressionMethod::None;

    $rgw = $writer->CreateRowGroup();
    foreach($columns as $column) {
      $rgw->WriteColumn($column);
    }
    $rgw->finish(); // TODO: check, if this has to move up into the foreach?
    $writer->finish();
  }

  /**
   * [ReadSingleRowGroupParquetFile description]
   * @param resource          $stream  [description]
   * @param Schema|null       &$schema  [description]
   * @param DataColumn[]|null &$columns [description]
   */
  public static function ReadSingleRowGroupParquetFile($stream, ?Schema &$schema, ?array &$columns): void
  {
    $reader = new ParquetReader($stream);
    $schema = $reader->schema;

    $rgr = $reader->OpenRowGroupReader(0);
    $dataFields = $schema->getDataFields();

    $columns = [];
    foreach($dataFields as $field) {
      $columns[] = $rgr->ReadColumn($field);
    }
  }

}
