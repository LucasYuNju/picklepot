package com.intel.picklepot.columnar;

import com.google.common.primitives.Primitives;
import com.intel.picklepot.metadata.Block;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import com.intel.picklepot.unsafe.Type;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

public class Utils {
  private static boolean enableDict;
  private static int dictBlockSizeThreshold;
  private static int initialSizePerCol;
  private static Unsafe unsafe;

  private Utils() {}

  static {
    configColumnWriter(true, 5 * 1024 * 1024, 1024 * 1024);
  }

  public static void configColumnWriter(boolean enableDict, int dictBlockSizeThreshold, int initialSizePerCol) {
    Utils.enableDict = enableDict;
    Utils.dictBlockSizeThreshold = dictBlockSizeThreshold;
    Utils.initialSizePerCol = initialSizePerCol;
  }

  public static ColumnWriter getColumnWriter(Class columnClazz, SimpleDataOutput output) {
    ValuesWriter valuesWriter;
    switch(classToType(columnClazz)) {
      case STRING:
        if (enableDict) {
          valuesWriter = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictBlockSizeThreshold, initialSizePerCol);
        }
        else {
          valuesWriter = new DeltaByteArrayWriter(initialSizePerCol);
        }
        break;
      case INT:
        valuesWriter = new DeltaBinaryPackingValuesWriter(initialSizePerCol);
        break;
      default:
        valuesWriter = new ObjectValuesWriter();
    }
    return new ColumnWriter(valuesWriter, output);
  }

  public static ColumnReader getColumnReader(Class columnClazz, SimpleDataInput input) {
    Block dataBlock = input.readBlock();
    Block dictBlock = dataBlock.getEncoding().usesDictionary() ? input.readBlock() : null;
    try {
      return new ColumnReader(dataBlock, dictBlock, columnClazz);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

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

  public static Type classToType(Class clazz) {
    if(clazz == String.class) {
      return Type.STRING;
    }
    if(clazz == Integer.class || clazz == int.class) {
      return Type.INT;
    }
    if(clazz.isPrimitive() || Primitives.isWrapperType(clazz) || clazz.isArray()) {
      return Type.UNSUPPRTED;
    }
    return Type.NESTED;
  }
}
