package com.intel.picklepot.columnar;

import com.google.common.primitives.Primitives;
import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.SimpleDataInput;
import com.intel.picklepot.io.SimpleDataOutput;
import com.intel.picklepot.serialization.FieldType;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.schema.PrimitiveType;
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

  public static ColumnWriter getColumnWriter(Class clazz, SimpleDataOutput output) {
    ValuesWriter valuesWriter;
    switch(toFieldType(clazz)) {
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
    return new ColumnWriter(valuesWriter, output, clazz);
  }

  public static ColumnReader getColumnReader(Class clazz, SimpleDataInput input) {
    ValuesReader valuesReader = null;
    Block dataBlock = input.readBlock();
    Block dictBlock = dataBlock.getEncoding().usesDictionary() ? input.readBlock() : null;

    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, getParquetType(clazz), 0, 0);
    Encoding encoding = dataBlock.getEncoding();
    try {
      if (encoding == Encoding.BIT_PACKED) {
        valuesReader = new ObjectValuesReader();
      } else if (encoding.usesDictionary()) {
        DictionaryPage dictPage = new DictionaryPage(BytesInput.from(dictBlock.getBytes()),
            dictBlock.getBytes().length, dictBlock.getNumValues(), dictBlock.getEncoding());
        Dictionary dict = encoding.initDictionary(descriptor, dictPage);
        valuesReader = encoding.getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dict);
      } else {
        valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
      }
      valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new ColumnReader(valuesReader, dataBlock, clazz);
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

  public static FieldType toFieldType(Class clazz) {
    if(clazz == String.class) {
      return FieldType.STRING;
    }
    if(clazz == Integer.class || clazz == int.class) {
      return FieldType.INT;
    }
    if(clazz.isPrimitive() || Primitives.isWrapperType(clazz) || clazz.isArray()) {
      return FieldType.UNSUPPRTED;
    }
    return FieldType.NESTED;
  }

  private static PrimitiveType.PrimitiveTypeName getParquetType(Class clazz) {
    if(clazz==String.class)
      return PrimitiveType.PrimitiveTypeName.BINARY;
    return PrimitiveType.PrimitiveTypeName.INT32;
  }
}
