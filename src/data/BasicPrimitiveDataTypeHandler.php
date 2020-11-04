<?php
namespace jocoon\parquet\data;

abstract class BasicPrimitiveDataTypeHandler extends BasicDataTypeHandler
{
  /**
   * @inheritDoc
   */
  public function unpackDefinitions(
    array $src,
    array $definitionLevels,
    int $maxDefinitionLevel,
    array &$hasValueFlags
  ): array {
    return $this->UnpackDefinitionsInternal($src, $definitionLevels, $maxDefinitionLevel, $hasValueFlags);
  }

  /**
   * [UnpackDefinitionsInternal description]
   * @param array $src                [description]
   * @param int[] $definitionLevels   [description]
   * @param int   $maxDefinitionLevel [description]
   * @param bool[] &$hasValueFlags      [description]
   * @return array
   */
  protected function UnpackDefinitionsInternal(array $src, array $definitionLevels, int $maxDefinitionLevel, array &$hasValueFlags) : array
  {
    // Micro-Optimization
    $definitionLevelCount = \count($definitionLevels);

    $result = \array_fill(0, $definitionLevelCount, null); // (TSystemType?[])GetArray(definitionLevels.Length, false, true);
    $hasValueFlags = \array_fill(0, $definitionLevelCount, false); // new bool[definitionLevels.Length];

    $isrc = 0;
    for ($i = 0; $i < $definitionLevelCount; $i++)
    {
      $level = $definitionLevels[$i];

      if ($level == $maxDefinitionLevel)
      {
        $result[$i] = $src[$isrc++];
        $hasValueFlags[$i] = true;
      }
      else if($level == 0)
      {
        $hasValueFlags[$i] = true;
      }
    }

    return $result;
  }

  /**
   * @inheritDoc
   */
  public function packDefinitions(
    array $data,
    int $maxDefinitionLevel,
    array &$definitions,
    int &$definitionsLength,
    int &$nullCount
  ): array {
    // TODO: check for nullables?

    return $this->packDefinitionsNullable($data, $maxDefinitionLevel, $definitions, $definitionsLength, $nullCount);
  }

  // private function PackDefinitions(
  //   array $data,
  //   int $maxDefinitionLevel,
  //   array &$definitions,
  //   int &$definitionsLength,
  //   int &$nullCount
  // ): array {
  //   $definitionLevels = array_fill(0, count($data), 0); // QUESTION/TODO: fill with null or 0?
  //   $definitionsLength = count($data);
  //
  //
  //   // nullCount = data.Count(i => !i.HasValue);
  //   $nullCount = count(array_filter($data, function($i) { return $i === null; }));
  //
  //   TSystemType[] result = new TSystemType[data.Length - nullCount];
  //   int ir = 0;
  //
  //   for(int i = 0; i < data.Length; i++)
  //   {
  //     TSystemType? value = data[i];
  //
  //     if(value == null)
  //     {
  //       definitionLevels[i] = 0;
  //     }
  //     else
  //     {
  //       definitionLevels[i] = maxDefinitionLevel;
  //       result[ir++] = value.Value;
  //     }
  //   }
  //
  //   return result;
  // }


}
