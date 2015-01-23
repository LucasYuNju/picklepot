package com.intel.picklepot.serialization;

public enum FieldType {
  INT,
  STRING,
  FLOAT,
  LONG,
  /**
   * 1.Primitive types excluding above types, like double
   * 2.Array, like Int[]
   */
  UNSUPPRTED,
  /**
   * Any type that can not be classified as above types, like Tuple2
   */
  NESTED
}
