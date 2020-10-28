<?php
namespace jocoon\parquet\helper;

use DateTimeImmutable;
use DateTimeInterface;

use jocoon\parquet\data\Schema;

class OtherExtensions
{
  /**
   * static initializer function
   * @return void
   */
  public static function initialize() {
    static::$UnixEpoch = DateTimeImmutable::createFromFormat('U', 0); // new DateTimeImmutable(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
  }

  /**
   * [private description]
   * @var DateTimeImmutable
   */
  private static $UnixEpoch = null;

  /**
   * [FromUnixDays description]
   * @param  int            $unixDays [description]
   * @return DateTimeInterface           [description]
   */
  public static function FromUnixDays(int $unixDays) : DateTimeInterface
  {
    return static::$UnixEpoch->setDate(1970, 1, 1 + $unixDays);
     // return UnixEpoch.AddDays(unixDays);
  }

  /**
   * [ToUnixDays description]
   * @param  DateTimeImmutable $dto [description]
   * @return int                    [description]
   */
  public static function ToUnixDays(DateTimeImmutable $dto): int
  {
    $diff = $dto->diff(static::$UnixEpoch);
    return $diff->days;
  }

  /**
   * [FromUnixMilliseconds description]
   * @param  int               $unixMilliseconds [description]
   * @return DateTimeInterface                   [description]
   */
  public static function FromUnixMilliseconds(int $unixMilliseconds) : DateTimeInterface
  {
    return static::$UnixEpoch->setTime(0,0,0,$unixMilliseconds * 1000);
    // return UnixEpoch.AddMilliseconds(unixMilliseconds);
  }

  /**
   * [ToUnixMilliseconds description]
   * @param  DateTimeImmutable $dto [description]
   * @return int                    [description]
   */
  public static function ToUnixMilliseconds(DateTimeImmutable $dto): int
  {
    $diff = $dto->diff(static::$UnixEpoch);
    $days = $diff->format('%a');
    $seconds = 0;
    if($days){
        $seconds += 24 * 60 * 60 * $days;
    }
    $hours = $diff->format('%H');
    if($hours){
        $seconds += 60 * 60 * $hours;
    }
    $minutes = $diff->format('%i');
    if($minutes){
        $seconds += 60 * $minutes;
    }
    $seconds += $diff->format('%s');
    $milliseconds = $seconds * 1000;
    return $milliseconds;
  }


  /**
   * [FromUnixMicroseconds description]
   * @param  int               $unixMicroseconds [description]
   * @return DateTimeInterface                   [description]
   */
  public static function FromUnixMicroseconds(int $unixMicroseconds) : DateTimeInterface
  {
    return static::$UnixEpoch->setTime(0,0,0,$unixMicroseconds);
    // return UnixEpoch.AddMilliseconds(unixMilliseconds);
  }


  /**
   * [AddPath description]
   * @param  string|null    $s     [description]
   * @param  string|array   $parts [description]
   * @return string         [description]
   */
  public static function AddPath(?string $s, $parts): string {
    $pathComponents = [];
    if($s !== null) {
      $pathComponents[] = $s;
    }

    if($parts !== null) {
      if(is_array($parts)) {
        $pathComponents = array_merge($pathComponents, array_filter($parts, function($p) { return $p !== null; }));
      } else {
        $pathComponents[] = $parts;
      }
    }

    return implode(Schema::PathSeparator, $pathComponents);
  }
}

// Make sure we perform the pseudo-static class init.
OtherExtensions::initialize();
