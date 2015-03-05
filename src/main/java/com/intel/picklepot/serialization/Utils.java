package com.intel.picklepot.serialization;

import org.apache.spark.SerializableWritable;
import org.apache.spark.util.collection.CompactBuffer;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
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

  public static Class<?> resolveClass(String className)
      throws IOException, ClassNotFoundException
  {
    try {
      return Class.forName(className, false, sun.misc.VM.latestUserDefinedLoader());
    } catch (ClassNotFoundException ex) {
      Class<?> cl = primClasses.get(className);
      if (cl != null) {
        return cl;
      } else {
        throw ex;
      }
    }
  }

  private static final HashMap<String, Class<?>> primClasses = new HashMap(8, 1.0F);
  static {
    primClasses.put("boolean", boolean.class);
    primClasses.put("byte", byte.class);
    primClasses.put("char", char.class);
    primClasses.put("short", short.class);
    primClasses.put("int", int.class);
    primClasses.put("long", long.class);
    primClasses.put("float", float.class);
    primClasses.put("double", double.class);
    primClasses.put("void", void.class);
  }
}
