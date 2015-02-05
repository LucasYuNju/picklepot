package com.intel.picklepot.serialization;

import org.apache.spark.SerializableWritable;
import org.apache.spark.util.collection.CompactBuffer;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class Utils {
  private static Unsafe unsafe;
  private static Set<Class> exceptions = new HashSet<Class>();

  static {
//    exceptions.add(MapStatus.class);
    exceptions.add(SerializableWritable.class);
    exceptions.add(CompactBuffer.class);
  }

  public static Unsafe unsafe() {
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

  public static boolean isException(Class clazz) {
    return exceptions.contains(clazz);
  }
}
