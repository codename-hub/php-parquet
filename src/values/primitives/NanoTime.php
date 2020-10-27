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
   * @param string $data   [description]
   * @param int    $offset [description]
   */
  public function __construct($data, int $offset)
  {
    // _timeOfDayNanos = BitConverter.ToInt64(data, offset);
    // _julianDay = BitConverter.ToInt32(data, offset + 8);
    $this->timeOfDayNanos = unpack("q", $data, $offset)[1];
    $this->julianDay = unpack("l",  $data, $offset + 8)[1];
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
