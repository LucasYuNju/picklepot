package com.intel.picklepot.column;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.serialization.Type;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;

import java.io.IOException;

public class Readers {
  public static class IntColumnReader extends ColumnReader {
    public IntColumnReader(DataInput input) {
      super(input, Type.INT);
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return valuesReader.readInteger();
    }
  }

  public static class LongColumnReader extends ColumnReader {
    public LongColumnReader(DataInput input) {
      super(input, Type.LONG);
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return valuesReader.readLong();
    }
  }

  public static class FloatColumnReader extends ColumnReader {
    public FloatColumnReader(DataInput input) {
      super(input, Type.FLOAT);
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return valuesReader.readFloat();
    }
  }

  public static class DoubleColumnReader extends ColumnReader {
    public DoubleColumnReader(DataInput input) {
      super(input, Type.DOUBLE);
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return valuesReader.readDouble();
    }
  }

  public static class StringColumnReader extends ColumnReader {
    public StringColumnReader(DataInput input) {
      this.dataBlock = input.readBlock();
      Block dictBlock = dataBlock.getEncoding().usesDictionary() ? input.readBlock() : null;
      ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, Type.STRING.toParquetType(), 0, 0);
      Encoding encoding = dataBlock.getEncoding();
      if (encoding.usesDictionary()) {
        Dictionary dict = getDictioinary(dictBlock, encoding, descriptor);
        valuesReader = encoding.getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dict);
      } else {
        valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
      }
      try {
        valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return new String(valuesReader.readBytes().getBytes());
    }
  }

  public static class ObjectColumnReader extends ColumnReader {
    public ObjectColumnReader(DataInput input) {
      this.dataBlock = input.readBlock();
      this.valuesReader = new ObjectValuesReader();
      try {
        valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      return ((ObjectValuesReader) valuesReader).readObject();
    }
  }

  public static class ArrayColumnReader extends ColumnReader {
    private ColumnReader lengthReader;
    private ColumnReader compReader;

    public ArrayColumnReader(DataInput input, Class clazz) {
      this.lengthReader = new IntColumnReader(input);
      Class compClazz = clazz.getComponentType();
      switch (Type.get(compClazz)) {
        case STRING:
          compReader = new StringColumnReader(input);
          break;
        case INT:
          compReader = new IntColumnReader(input);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public Object read() {
      if(!hasNext()) {
        return null;
      }
      Integer len = (Integer) lengthReader.read();
      Object[] ret = new Object[len];
      for(int i=0; i<len; i++) {
        ret[i] = compReader.read();
      }
      return ret;
    }
  }
}
