<?php
namespace codename\parquet\helper;

use codename\parquet\data\Schema;

use codename\parquet\format\ColumnChunk;

class ThriftExtensions
{
  /**
   * [GetPath description]
   * @param  ColumnChunk $columnChunk [description]
   * @return string                   [description]
   */
  public static function GetPath(ColumnChunk $columnChunk) : string
  {
     return implode(Schema::PathSeparator, $columnChunk->meta_data->path_in_schema);
  }
}
