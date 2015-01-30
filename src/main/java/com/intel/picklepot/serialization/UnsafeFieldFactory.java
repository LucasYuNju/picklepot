package com.intel.picklepot.serialization;

public class UnsafeFieldFactory {
  /**
   * object may be auto-boxed, and it's probable that clazz != object.getClass().
   * @param clazz field type
   * @param object one of field objects
   * @param offset field offset
   * @return
   */
  public static UnsafeField getUnsafeField(Class clazz, Object object, long offset) {
    switch (Type.get(clazz)) {
      case INT:
        return new UnsafeIntField(clazz, offset);
      case STRING:
        return new UnsafeStringField(clazz, offset);
      case LONG:
        return new UnsafeLongField(clazz, offset);
      case FLOAT:
        return new UnsafeFloatField(clazz, offset);
      case DOUBLE:
        return new UnsafeDoubleField(clazz, offset);
      case ARRAY:
        return new UnsafeArrayField(clazz, offset);
      case NESTED:
        return new UnsafeNestedField(object, offset);
      case UNSUPPORTED:
        return new UnsafeUnsupportedField(clazz, offset);
      default:
        throw new IllegalArgumentException("object is of wrong TYPE:" + Type.get(object.getClass()));
    }
  }
}
