<?php
declare(strict_types=1);
namespace codename\parquet\tests;

final class QuirksTest extends TestBase
{
  /**
   * Test to make sure PHP's strcmp()
   * behaves similar to C#'s string.CompareOrdinal()
   */
  public function testStringCompareOrdinal(): void {

    $o = 'one';
    $a = 'two';
    $b = 'three';
    $c = 'zz';
    $d = 'zzzz';
    $e = 'zzzzz';

    $this->assertLessThan(0, strcmp($o, $a));
    $this->assertLessThan(0, strcmp($o, $b));

    $this->assertGreaterThan(0, strcmp($a, $b));
    $this->assertLessThan(0, strcmp($b, $a));
    $this->assertLessThan(0, strcmp($a, $c));
    $this->assertLessThan(0, strcmp($b, $c));

    $this->assertLessThan(0, strcmp($a, $d));
    $this->assertLessThan(0, strcmp($b, $d));
    $this->assertLessThan(0, strcmp($b, $e));

    $x = 'one';
    $z = 'three';

    $this->assertLessThan(0, strcmp($x, $z));
  }
}
