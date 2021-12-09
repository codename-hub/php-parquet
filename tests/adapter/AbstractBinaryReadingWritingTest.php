<?php
declare(strict_types=1);
namespace codename\parquet\tests\adapter;

use codename\parquet\adapter\BinaryReader;
use codename\parquet\adapter\BinaryWriter;
use codename\parquet\adapter\BinaryReaderInterface;
use codename\parquet\adapter\BinaryWriterInterface;

abstract class AbstractBinaryReadingWritingTest extends \PHPUnit\Framework\TestCase {

  /**
   * [getBinaryReader description]
   * @param  resource $stream
   * @return BinaryReaderInterface
   */
  protected abstract function getBinaryReader($stream): BinaryReaderInterface;

  /**
   * [getBinaryWriter description]
   * @param  resource $stream
   * @return BinaryWriterInterface         [description]
   */
  protected abstract function getBinaryWriter($stream): BinaryWriterInterface;

  /**
   * @testWith [ true  ]
   *           [ false ]
   * @param bool $littleEndian  [description]
   */
  public function testWriteReadAllTypes(bool $littleEndian): void {
    $ms = fopen('php://memory', 'r+');

    $writer = $this->getBinaryWriter($ms);
    $writer->setByteOrder($littleEndian ? BinaryWriter::LITTLE_ENDIAN : BinaryWriter::BIG_ENDIAN);
    // $reader = $this->getBinaryReader($ms);
    // $reader->setByteOrder($littleEndian ? BinaryReader::LITTLE_ENDIAN : BinaryReader::BIG_ENDIAN);


    $datasets = [
      'Byte' => [
        'values'  => [ 0x01, 0x02, 0x05 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeByte($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readByte(); },
      ],

      // Byte-reading and -writing using CustomBinary* is inconsistent by design
      // 'Bytes' => [
      //   'values'  => [ [0x01, 0x02], [ 0x02, 0x03 ], [ 0x05 ] ],
      //   'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeBytes($v); },
      //   'read'    => function(BinaryReaderInterface $reader, $v) { return $reader->readBytes(count($v)); },
      // ],

      // 'Int8' => [
      //   'values'  => [ -5, 5, 127 ],
      //   'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeInt8($v); },
      //   'read'    => function(BinaryReaderInterface $reader) { return $reader->readInt8(); },
      // ],
      // 'UInt8' => [
      //   'values'  => [ 0, 5, 127 ],
      //   'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeUInt8($v); },
      //   'read'    => function(BinaryReaderInterface $reader) { return $reader->readUInt8(); },
      // ],

      'Int16' => [
        'values'  => [ -5, 5, 127 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeInt16($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readInt16(); },
      ],
      'UInt16' => [
        'values'  => [ 0, 5, 1024 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeUInt16($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readUInt16(); },
      ],

      'Int32' => [
        'values'  => [ -2147483648, -200000, 5, 0, 2147483647 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeInt32($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readInt32(); },
      ],
      'UInt32' => [
        'values'  => [ 5, 0, 2147483647, 2147483648, 4294967295 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeUInt32($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readUInt32(); },
      ],

      'Int64' => [
        'values'  => [ PHP_INT_MIN, -200000, 5, 0, 2147483647, PHP_INT_MAX ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeInt64($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readInt64(); },
      ],

      'Float' => [
        'values'  => [ -5.1, 0.0, 0.1, 2500.123 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeSingle($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readSingle(); },
      ],
      'Double' => [
        'values'  => [ -500000.1, 0.0, 0.1, 2500.1231231 ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeDouble($v); },
        'read'    => function(BinaryReaderInterface $reader) { return $reader->readDouble(); },
      ],
      'String' => [
        'values'  => [ ' ', 'A', '\\', chr(127), chr(0) ],
        'write'   => function(BinaryWriterInterface $writer, $v) { $writer->writeString($v); },
        'read'    => function(BinaryReaderInterface $reader, $v) { return $reader->readString(strlen($v)); },
      ]
    ];

    foreach($datasets as $type => $config) {
      foreach($config['values'] as $v) {
        $config['write']($writer, $v);
      }
    }


    // Rewind the current stream
    fseek($ms, 0);

    // NOTE: some readers may buffer, internally
    // so we have to init the Reader after finishing the write process.
    $reader = $this->getBinaryReader($ms);
    $reader->setByteOrder($littleEndian ? BinaryReader::LITTLE_ENDIAN : BinaryReader::BIG_ENDIAN);

    // make sure to rewind the reader, just to make sure.
    $reader->setPosition(0);

    foreach($datasets as $type => $config) {
      foreach($config['values'] as $v) {
        if(is_float($v)) {
          $this->assertEqualsWithDelta($v, $config['read']($reader), 0.0001, $type);
        } else {
          // We supply $v only for max length to be read (strings and bytes, in theory)
          $this->assertEquals($v, $config['read']($reader, $v), $type);
        }
      }
    }
  }

}
