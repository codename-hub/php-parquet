<?php
namespace jocoon\parquet\file;

use Thrift\Protocol\TCompactProtocol;

use jocoon\parquet\transport\TStreamTransport;

class ThriftStream {

  /**
   * [protected description]
   * @var resource
   */
  protected $_s = null;

  /**
   * [private description]
   * @var TCompactProtocol
   */
  private $_protocol;

  public function __construct($s)
  {
    $this->_s = $s;
    $transport = new TStreamTransport($s, $s);
    $this->_protocol = new TCompactProtocol($transport);
  }

  public function Read($type) {
    $res = new $type();
    // echo(chr(10)."ThriftStream::Read($type) @ ".ftell($this->_s).chr(10));
    $res->Read($this->_protocol);
    return $res;
  }

  public function ReadByInstance($instance) {
    // echo(chr(10)."ThriftStream::ReadByInstance(".get_class($instance).") @ ".ftell($this->_s).chr(10));
    return $instance->Read($this->_protocol);
    // return $instance;
  }

  public function Write($type, $obj, bool $rewind = false) : int {
    // $this->_s
    $success = fflush($this->_s);

    // DEBUG
    if(!$success) {
      die("WRITE ERROR");
    }

    $startPos = ftell($this->_s);
    $obj->Write($this->_protocol);
    $success = fflush($this->_s);

    // TODO: handle write errors

    // // DEBUG
    // if(!$success) {
    //   die("WRITE ERROR");
    // }

    $size = ftell($this->_s) - $startPos;
    if($rewind) {
      fseek($this->_s, $startPos, SEEK_SET);
    }
    return (int)$size;
  }



}
