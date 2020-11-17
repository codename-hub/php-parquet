<?php
namespace jocoon\parquet\values\primitives;

use DateTimeImmutable;

class NanoTime
{
  /**
   * [private description]
   * @var int
   */
  private $julianDay;

  /**
   * [private description]
   * @var int
   */
  private $timeOfDayNanos;

  /**
   * [__construct description]
   * @param string      $data   [description]
   * @param int|null    $offset [description]
   */
  public function __construct($data = null, ?int $offset = null)
  {
    // _timeOfDayNanos = BitConverter.ToInt64(data, offset);
    // _julianDay = BitConverter.ToInt32(data, offset + 8);
    if($data !== null && $offset !== null) {
      $this->timeOfDayNanos = unpack("q", $data, $offset)[1];
      $this->julianDay = unpack("l",  $data, $offset + 8)[1];
    }
  }

  /**
   * Creates a NanoTime object from a DateTimeImmutable
   * @param  DateTimeImmutable $dt [description]
   * @return NanoTime              [description]
   */
  public static function NanoTimeFromDateTimeImmutable(DateTimeImmutable $dt) : NanoTime {
    $m = (int)$dt->format('n'); // month without leading zero
    $d = (int)$dt->format('j'); // day without leading zero
    $y = (int)$dt->format('Y'); // year

    if($m < 3) {
      $m = $m + 12;
      $y = $y - 1;
    }

    $nanoTime = new self();
    // NOTE: we have to perform some integer casting - otherwise, we get differences and an internal float-based discrepancy
    $nanoTime->julianDay = (int)($d + (int)(153 * $m - 457) / 5 + 365 * $y + (int)($y / 4) - (int)($y / 100) + (int)($y / 400) + 1721119);

    $seconds = $dt->format('H') * 60 * 60
      + $dt->format('i') * 60
      + $dt->format('s');
    $nanoTime->timeOfDayNanos = (($seconds * 1000000000) + ($dt->format('u') * 1000));

    return $nanoTime;
  }

  /**
   * [Write description]
   * @param \jocoon\parquet\adapter\BinaryWriter $writer [description]
   */
  public function Write(\jocoon\parquet\adapter\BinaryWriter $writer):void {
    $writer->writeInt64($this->timeOfDayNanos);
    $writer->writeInt32($this->julianDay);
  }

  /**
   * [DateTimeImmutableFromNanoTimeBytes description]
   * @param  string            $data   [description]
   * @param  int               $offset [description]
   * @return DateTimeImmutable         [description]
   */
  public static function DateTimeImmutableFromNanoTimeBytes($data, int $offset) : DateTimeImmutable {
    $timeOfDayNanos = unpack("q", $data, $offset)[1];
    $julianDay = unpack("l",  $data, $offset + 8)[1];

    $L = $julianDay + 68569;
    $N = (int)((4 * $L) / 146097);
    $L = $L - ((int)((146097 * $N + 3) / 4));
    $I = (int)((4000 * ($L + 1) / 1461001));
    $L = $L - (int)((1461 * $I) / 4) + 31;
    $J = (int)((80 * $L) / 2447);
    $Day = (int)($L - (int)((2447 * $J) / 80));
    $L = (int)($J / 11);
    $Month = (int)($J + 2 - 12 * $L);
    $Year = (int)(100 * ($N - 49) + $I + $L);

    // $ms = $timeOfDayNanos / (double)1000000;
    // $microseconds = (int)($ms / 1000);

    $microseconds = (int) ($timeOfDayNanos / (double)1000);

    $result = new \DateTime();
    $result
      ->setTimezone(static::$DateTimeZone_UTC ?? static::$DateTimeZone_UTC = new \DateTimeZone('UTC'))
      ->setDate($Year, $Month, $Day)
      ->setTime(0, 0, 0, $microseconds);

    return \DateTimeImmutable::createFromMutable($result);
  }

  static $DateTimeZone_UTC = null;

  public static function getDateTimeObject(NanoTime $nanoTime)
  {
    $L = $nanoTime->julianDay + 68569;
    $N = (int)((4 * $L) / 146097);
    $L = $L - ((int)((146097 * $N + 3) / 4));
    $I = (int)((4000 * ($L + 1) / 1461001));
    $L = $L - (int)((1461 * $I) / 4) + 31;
    $J = (int)((80 * $L) / 2447);
    $Day = (int)($L - (int)((2447 * $J) / 80));
    $L = (int)($J / 11);
    $Month = (int)($J + 2 - 12 * $L);
    $Year = (int)(100 * ($N - 49) + $I + $L);

    // $ms = $nanoTime->_timeOfDayNanos / (double)1000000;
    // $microseconds = (int)($ms / 1000);

    $microseconds = (int) ($nanoTime->_timeOfDayNanos / (double)1000);

    $result = new \DateTime();
    $result
      ->setTimezone(static::$DateTimeZone_UTC ?? static::$DateTimeZone_UTC = new \DateTimeZone('UTC')) // ?
      ->setDate($Year, $Month, $Day)
      ->setTime(0, 0, 0, $microseconds);

    // var result = new DateTimeOffset(Year, Month, Day,
    //   0, 0, 0,
    //   TimeSpan.Zero);
    // result = result.AddMilliseconds(ms);

    return \DateTimeImmutable::createFromMutable($result);
  }

  // public NanoTime(DateTimeOffset dt)
  // {
  //    int m = dt.Month;
  //    int d = dt.Day;
  //    int y = dt.Year;
  //
  //    if (m < 3)
  //    {
  //       m = m + 12;
  //       y = y - 1;
  //    }
  //
  //    _julianDay = d + (153 * m - 457) / 5 + 365 * y + (y / 4) - (y / 100) + (y / 400) + 1721119;
  //    _timeOfDayNanos = (long)(dt.TimeOfDay.TotalMilliseconds * 1000000D);
  // }
}
