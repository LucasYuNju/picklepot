package com.intel.picklepot.unsafe;

public enum FieldType {
  /**
   * int or Integer
   */
  INT,
  /**
   * String
   */
  STRING,
  /**
   * 1.Primitive types excluding INT and STRING, like Long and float
   * 2.Array, like Int[]
   */
  UNSUPPRTED,
  /**
   * Any type that can not be classified as above types, like Tuple2
   */
  NESTED
}
