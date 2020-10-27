<?php
namespace jocoon\parquet;

interface StreamWrapperInterface
{
  /* Methoden */
  function __construct ();
  function __destruct ();
  function dir_closedir () : bool;
  function dir_opendir ( string $path , int $options ) : bool;
  function dir_readdir ( ) : string;
  function dir_rewinddir ( ) : bool;
  function mkdir ( string $path , int $mode , int $options ) : bool;
  function rename ( string $path_from , string $path_to ) : bool;
  function rmdir ( string $path , int $options ) : bool;
  function stream_cast ( int $cast_as );
  function stream_close (  ) : void;
  function stream_eof (  ) : bool;
  function stream_flush (  ) : bool;
  function stream_lock ( int $operation ) : bool;
  function stream_metadata ( string $path , int $option , $value ) : bool;
  function stream_open ( string $path , string $mode , int $options , string &$opened_path ) : bool;
  function stream_read ( int $count ) : string;
  function stream_seek ( int $offset , int $whence = SEEK_SET ) : bool;
  function stream_set_option ( int $option , int $arg1 , int $arg2 ) : bool;
  function stream_stat (  ) : array;
  function stream_tell (  ) : int;
  function stream_truncate ( int $new_size ) : bool;
  function stream_write ( string $data ) : int;
  function unlink ( string $path ) : bool;
  function url_stat ( string $path , int $flags ) : array;

}
