package com.intel.picklepot.serialization;

import com.google.common.primitives.Primitives;
import parquet.schema.PrimitiveType;

public enum Type {
  INT(PrimitiveType.PrimitiveTypeName.INT32),

  STRING(PrimitiveType.PrimitiveTypeName.BINARY),

  FLOAT(PrimitiveType.PrimitiveTypeName.FLOAT),

  LONG(PrimitiveType.PrimitiveTypeName.INT64),

  /**
   * 1.Primitive types excluding above types, like double
   * 2.Array, like Int[]
   */
  UNSUPPORTED(null),

  /**
   * Any type that can not be classified as above types, like Tuple2
   */
  NESTED(null);

  private PrimitiveType.PrimitiveTypeName parquetType;

  Type(PrimitiveType.PrimitiveTypeName parquetType) {
    this.parquetType = parquetType;
  }

  public PrimitiveType.PrimitiveTypeName getParquetType() {
    return parquetType;
  }

  public static Type get(Class clazz) {
    if(clazz == String.class) {
      return Type.STRING;
    }
    if(clazz == Integer.class || clazz == int.class) {
      return Type.INT;
    }
    if(clazz.isPrimitive() || Primitives.isWrapperType(clazz) || clazz.isArray()) {
      return Type.UNSUPPORTED;
    }
    return Type.NESTED;
  }
}
