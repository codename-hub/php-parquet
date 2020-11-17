<?php
declare(strict_types=1);
namespace jocoon\parquet\tests;
use PHPUnit\Framework\TestCase;

use jocoon\parquet\ParquetReader;
use jocoon\parquet\ParquetWriter;
use jocoon\parquet\ParquetOptions;
use jocoon\parquet\CompressionMethod;
use jocoon\parquet\ParquetExtensions;

use jocoon\parquet\data\Schema;
use jocoon\parquet\data\DataField;
use jocoon\parquet\data\DataColumn;

class TestBase extends TestCase {

  /**
   * [openTestFile description]
   * @param  string $name [description]
   * @return mixed  [file handle]
   */
  protected function openTestFile(string $name) {
    return fopen(__DIR__ . '/data/' . $name, 'r');
  }

  /**
   *
   * @param DataField           $field      [description]
   * @param DataColumn          $dataColumn [description]
   * @param ParquetOptions|null $options
   * @return DataColumn|null
   */
  protected function WriteReadSingleColumn(DataField $field, DataColumn $dataColumn, ?ParquetOptions $options = null): ?DataColumn
  {
    $ms = fopen('php://memory', 'r+');

      // write with built-in extension method
    ParquetExtensions::WriteSingleRowGroupParquetFile($ms, new Schema([$field]), [$dataColumn], $options);
    fseek($ms, 0);

    // read first gow group and first column
    $reader = new ParquetReader($ms);
    if($reader->getRowGroupCount() === 0) return null;

    $rgReader = $reader->OpenRowGroupReader(0);
    return $rgReader->ReadColumn($field);
  }

  /**
   * [WriteReadSingleRowGroup description]
   * @param  Schema           $schema     [description]
   * @param  DataColumn[]     $columns    [description]
   * @param  Schema|null      &$readSchema [description]
   * @return DataColumn[]
   */
  protected function WriteReadSingleRowGroup(Schema $schema, array $columns, ?Schema &$readSchema): array
  {
    $ms = fopen('php://memory', 'r+');

    ParquetExtensions::WriteSingleRowGroupParquetFile($ms, $schema, $columns);
    fseek($ms, 0);

    //System.IO.File.WriteAllBytes("c:\\tmp\\1.parquet", ms.ToArray());

    $reader = new ParquetReader($ms);

    $readSchema = $reader->schema;
    $rgReader = $reader->OpenRowGroupReader(0);

    $returnval = [];
    foreach($columns as $c) {
      $returnval[] = $rgReader->ReadColumn($c->getField());
    }
    return $returnval;

      // using (ParquetRowGroupReader rgReader = reader.OpenRowGroupReader(0))
      // {
      //   return columns.Select(c =>
      //   rgReader.ReadColumn(c.Field))
      //   .ToArray();
      //
      // }

  }

  /**
   * [WriteReadSingle description]
   * @param DataField $field             [description]
   * @param mixed     $value             [description]
   * @param int       $compressionMethod [description]
   */
  protected function WriteReadSingle(DataField $field, $value, int $compressionMethod = CompressionMethod::None)
  {
    //for sanity, use disconnected streams
    // byte[] data;

    $ms = fopen('php://memory', 'r+');

    // write single value
    $writer = new ParquetWriter(new Schema([$field]), $ms);
    $writer->compressionMethod = $compressionMethod;

    $rg = $writer->CreateRowGroup();
    $column = new DataColumn($field, [ $value ]);
    $rg->WriteColumn($column);
    $rg->finish();
    $writer->finish();
    // $writer = null;

    $data = stream_get_contents($ms, -1, 0);

    fclose($ms);

      //   using (ParquetRowGroupWriter rg = writer.CreateRowGroup())
      //   {
      //     Array dataArray = Array.CreateInstance(field.ClrNullableIfHasNullsType, 1);
      //     dataArray.SetValue(value, 0);
      //     var column = new DataColumn(field, dataArray);
      //
      //     rg.WriteColumn(column);
      //   }
      // }
      //
      // data = ms.ToArray();

    $ms = fopen('php://memory', 'r+');

    fwrite($ms, $data);
    fflush($ms);

    // read back single value
    fseek($ms, 0);



    $reader = new ParquetReader($ms);
    $rowGroupReader = $reader->OpenRowGroupReader(0);
    $column = $rowGroupReader->ReadColumn($field);
    return $column->getData()[0];

    //
    //
    // using (var ms = new MemoryStream(data))
    // {
    //   // read back single value
    //
    //   ms.Position = 0;
    //   using (var reader = new ParquetReader(ms))
    //   {
    //     using (ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(0))
    //     {
    //       DataColumn column = rowGroupReader.ReadColumn(field);
    //
    //       return column.Data.GetValue(0);
    //     }
    //   }
    // }
  }


}
