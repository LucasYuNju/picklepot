package com.intel.picklepot.serialization;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class Utils {
  private static Unsafe unsafe;

  public static Unsafe getUnsafe() {
    if(unsafe == null) {
      try {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        unsafe = (Unsafe) theUnsafe.get(null);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
    }
    return unsafe;
  }
}
