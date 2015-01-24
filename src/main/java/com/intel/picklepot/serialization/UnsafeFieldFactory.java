package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;

public class UnsafeFieldFactory {
  public static UnsafeField getUnsafeField(Class clazz, Object object, long offset, PicklePotImpl picklePot) {
    switch (Type.get(object.getClass())) {
      case INT:
        return new UnsafeIntField(clazz, offset, picklePot);
      case STRING:
        return new UnsafeStringField(clazz, offset, picklePot);
      case ARRAY:
        return new UnsafeArrayField(clazz, offset, picklePot);
      case NESTED:
        return new UnsafeNestedField(object, offset, picklePot);
      case UNSUPPORTED:
        return new UnsafeUnsupportedField(clazz, offset, picklePot);
      default:
        throw new IllegalArgumentException("called with wrong TYPE:" + Type.get(object.getClass()));
    }
  }
}
