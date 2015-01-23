package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;

public class UnsafeFieldFactory {
  public static UnsafeField getUnsafeField(Class clazz, Object object, long offset, PicklePotImpl picklePot, boolean directAccess) {
    switch (Type.get(object.getClass())) {
      case INT:
        return new UnsafeIntField(clazz, offset, picklePot, directAccess);
      case STRING:
        return new UnsafeStringField(clazz, offset, picklePot, directAccess);
      case ARRAY:
        return new UnsafeArrayField(clazz, offset, picklePot, directAccess);
      case NESTED:
        return new UnsafeNestedField(object, offset, picklePot, directAccess);
      case UNSUPPORTED:
        return new UnsafeUnsupportedField(clazz, offset, picklePot, directAccess);
      default:
        throw new IllegalArgumentException("called with wrong TYPE:" + Type.get(object.getClass()));
    }
  }
}
