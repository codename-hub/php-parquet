<?php
declare(strict_types=1);
namespace codename\parquet\tests\Reader;

use codename\parquet\ParquetReader;
use codename\parquet\ParquetOptions;

use codename\parquet\data\DataField;
use codename\parquet\data\DataColumn;

use codename\parquet\tests\TestBase;

class ParquetCsvComparison extends TestBase
{
  /**
   * [CompareFiles description]
   * @param string $baseName               [description]
   * @param string $encoding               [description]
   * @param bool   $treatByteArrayAsString [description]
   * @param array  $columnTypes            [description]
   */
  protected function CompareFiles(string $baseName, string $encoding, bool $treatByteArrayAsString, array $columnTypes): void {
    $parquet = $this->ReadParquet("{$baseName}.{$encoding}.parquet", $treatByteArrayAsString);
    $csv = $this->ReadCsv("{$baseName}.csv");
    $this->Compare($parquet, $csv, $columnTypes);
  }

  /**
   * [Compare description]
   * @param DataColumn[]  $parquet     [description]
   * @param DataColumn[]  $csv         [description]
   * @param array         $columnTypes [description]
   */
  protected function Compare(array $parquet, array $csv, array $columnTypes): void {
    // compare number of columns is the same
    $this->assertEquals(count($parquet), count($csv));

    // compare column names
    for($i = 0; $i < count($parquet); $i++)
    {
      $found = false;
      foreach($csv as $dc) {
        if($dc->getField()->name == $parquet[$i]->getField()->name) {
          $found = true;
          break;
        }
      }

      $this->assertTrue($found, 'Column name found');
      // Assert.Contains(csv, dc => dc.Field.Name == parquet[i].Field.Name);
    }



    //compare column values one by one
    for($ci = 0; $ci < count($parquet); $ci++)
    {
      $pc = $parquet[$ci];
      $cc = $csv[$ci];

      for($ri = 0; $ri < count($pc->getData()); $ri++)
      {
        // Type clrType = pc.Field.ClrType;
        $phpType = $pc->getField()->phpType;
        $phpClass = $pc->getField()->phpClass;
        // object pv = pc.Data.GetValue(ri);
        $pv = $pc->getData()[$ri];

        $cv = $this->ChangeType($cc->getData()[$ri], $phpType, $phpClass);
        // object cv = ChangeType(cc.Data.GetValue(ri), clrType);

        // print_r([
        //   'phpType' => $phpType,
        //   'pv' => $pv,
        //   'cv_raw' => $cc->getData()[$ri],
        //   'cv' => $cv,
        //   'pv_type' => gettype($pv),
        //   'cv_type' => gettype($cv),
        // ]);
        // var_dump($cc->getData()[$ri]);

        if ($pv === null)
        {
          $isCsvNull =
             $cv === null ||
             (is_string($cv) && empty($cv)); // TODO: check if this is right...

          if(!$isCsvNull) {
            print_r([
              'phpType' => $phpType,
              'pv' => $pv,
              'cv_raw' => $cc->getData()[$ri],
              'cv' => $cv,
              'pv_type' => gettype($pv),
              'cv_type' => gettype($cv),
            ]);
            var_dump($cc->getData()[$ri]);
            die();
          }

          $this->assertTrue($isCsvNull, "expected null value in column {$pc->getField()->name}, value #{$ri}");
        }
        else
        {
          if ($phpType == 'string')
          {
            $this->assertTrue(trim($pv) == trim($cv), "expected {$cv} but was {$pv} in column {$pc->getField()->name}, value #{$ri}");
          }
          // else if ($phpType == typeof(byte[]))
          // {
          //    byte[] pva = (byte[]) pv;
          //    byte[] cva = (byte[]) cv;
          //    Assert.True(pva.Length == cva.Length, $"expected length {cva.Length} but was {pva.Length} in column {pc.Field.Name}, value #{ri}");
          //    for (int i = 0; i < pva.Length; i++)
          //    {
          //       Assert.True(pva[i] == cva[i], $"expected {cva[i]} but was {pva[i]} in column {pc.Field.Name}, value #{ri}, array index {i}");
          //    }
          // }
          else
          {
            //
            // if($pv != $cv) {
            //   var_dump($pc->getField());
            //   var_dump($pv);
            //   var_dump($cv);
            //
            // }
            // if($pv instanceof \DateTimeImmutable) {
            //   var_dump($pv);
            //   var_dump($cv);
            // }

            $cvFormatted = print_r($cv, true);
            $pvFormatted = print_r($pv, true);
            $this->assertEquals($cv, $pv, "expected {$cvFormatted} but was {$pvFormatted} in column {$pc->getField()->name}, value #{$ri}");
          }
        }
      }
    }
  }

  protected function ChangeType($v, $t, $class)
  {
    if ($v === null) return null;
    if (gettype($v) == $t) return $v;
    if (is_string($v) && $v === '') return null;

    if($class == \DateTimeImmutable::class)
    {
      $so = (string)$v;

      // // Workaround for PHP not interpreting a trailing Z as UTC designator?
      // if(substr($so, -1) === 'Z') {
      //   $dtz = new \DateTimeZone('UTC');
      //   $so = substr($so, 0, -1);
      //   $dto = new \DateTimeImmutable($so, $dtz);
      // } else {
      //   $dto = new \DateTimeImmutable($so);
      // }

      return new \DateTimeImmutable($so);

      // DEBUG
      // echo("ChangeType:");
      // print_r($dto);
      // var_dump($dto);
      // if($dto['timezone'] === 'Z') {
      //   $dto->setTimezone(new \DateTimeZone('UTC'));
      // }
      // return $dto;
    }

    // shouldn't be needed
    // if ($t == typeof(byte[]))
    // {
    //   string so = (string) v;
    //   return Encoding.UTF8.GetBytes(so);
    // }

    switch($t) {
      case 'integer':
        return intval($v);
      case 'float':
        return floatval($v);
      case 'double':
        return doubleval($v);
      case 'boolean':
        // Workaround for boolval not converting "false" to false.
        // Instead, a non-empty string is returned as truthy, which is wrong for this case.
        if($v === 'false') {
          return false;
        }
        return boolval($v);
      case 'object':
        if($class) {
          return new $class($v);
        } else {
          // missing info?
          return $v; // TESTING
        }
      default:
        throw new \Exception('to be extended: '.$t." v: ". $v);
    }

    // return Convert.ChangeType(v, t);
  }

  /**
   * [ReadParquet description]
   * @param  string $name                   [description]
   * @param  bool   $treatByteArrayAsString [description]
   * @return DataColumn[]                   [description]
   */
  protected function ReadParquet(string $name, bool $treatByteArrayAsString): array
  {
    $s = $this->openTestFile($name);
    $po = new ParquetOptions();
    $po->TreatByteArrayAsString = $treatByteArrayAsString;
    $pr = new ParquetReader($s, $po);

    $rgr = $pr->OpenRowGroupReader(0);

    $result = [];
    foreach($pr->schema->getDataFields() as $df) {
      $result[] = $rgr->ReadColumn($df);
    }

    return $result;
  }

  /**
   * [ReadCsv description]
   * @param  string $name [description]
   * @return DataColumn[]                   [description]
   */
  protected function ReadCsv(string $name): array
  {
    $columns = [];
    $columnNames = null;

    $fs = $this->openTestFile($name);

    // header
    $columnNames = fgetcsv($fs, 0, ",", '"', '\\');

    // values
    while($values = fgetcsv($fs, 0, ",", '"', '\\')) {
      foreach ($values as $i => $value) {
        $column = &$columns[$i];
        $column[] = $value;
      }
    }

    $result = [];
    foreach($columnNames as $i => $n) {
      $result[] = new DataColumn(DataField::createFromType($n, 'string'), $columns[$i]);
    }

    return $result;
  }

}
