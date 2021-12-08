<?php
namespace codename\parquet\helper;

/**
 * Class for diving into (assoc) array structures
 * and retrieving or setting data
 */
class DeepAccess {

  /**
   * This is a purely static helper class
   * and *MUST NOT* be initialized as an instance
   * This will throw an error exception, as you cannot
   * call this private constructor
   */
  private function __construct()
  {
  }

  /**
   * deeply access a key in a nested array or object
   *
   * @param  mixed|array|object $obj  [description]
   * @param  array              $keys [description]
   * @return mixed|null
   */
  public static function get($obj, array $keys) {
    $dive = &$obj;
    foreach($keys as $key) {
      if(isset($dive[$key])) {
        $dive = &$dive[$key];
      } else {
        $dive = null;
        break;
      }
    }
    return $dive;
  }

  /**
   * [set description]
   * @param [type] $obj   [description]
   * @param array  $keys  [description]
   * @param [type] $value [description]
   */
  public static function set($obj, array $keys, $value) {
    $dive = &$obj;
    foreach($keys as $key) {
      if(!\is_array($dive)) {
        $dive = [ $key => true ];
      }
      if(isset($dive[$key])) {
        $dive = &$dive[$key];
      } else {
        $dive[$key] = true;
        $dive = &$dive[$key];
      }
    }
    // finally set value at path
    $dive = $value;
    return $obj;
  }

}
