package com.intel.picklepot.column;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;
import com.intel.picklepot.serialization.Type;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.column.values.plain.BooleanPlainValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.api.Binary;

import java.lang.reflect.Array;

public class Writers {
  public static class IntColumnWriter extends ColumnWriter {
    public IntColumnWriter(DataOutput output) {
      super(output);
      this.valuesWriter = new DeltaBinaryPackingValuesWriter(Settings.initialColSize);
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeInteger((Integer) value);
      statistics.add(value);
    }
  }

  public static class LongColumnWriter extends ColumnWriter {
    public LongColumnWriter(DataOutput output) {
      super(output);
//      valuesWriter = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(Settings.dictSizeThreshold, Settings.initialColSize);
      valuesWriter = new PlainValuesWriter(Settings.initialColSize);
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeLong((Long) value);
    }
  }

  public static class BooleanColumnWriter extends ColumnWriter {
    public BooleanColumnWriter(DataOutput output) {
      super(output);
      this.valuesWriter = new BooleanPlainValuesWriter();
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeBoolean((Boolean) value);
      statistics.add(value);
    }
  }


  public static class FloatColumnWriter extends ColumnWriter {

    public FloatColumnWriter(DataOutput output) {
      super(output);
//      valuesWriter = new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(Settings.dictSizeThreshold, Settings.initialColSize);
      valuesWriter = new PlainValuesWriter(Settings.initialColSize);
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeFloat((Float) value);
    }
  }

  public static class DoubleColumnWriter extends ColumnWriter {

    public DoubleColumnWriter(DataOutput output) {
      super(output);
//      valuesWriter = return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(Settings.dictSizeThreshold, Settings.initialColSize);
      valuesWriter = new PlainValuesWriter(Settings.initialColSize);
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeDouble((Double) value);
    }
  }

  public static class StringColumnWriter extends ColumnWriter {
    public StringColumnWriter(DataOutput output) {
      super(output);
      if (Settings.enableDict) {
        this.valuesWriter = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(Settings.dictSizeThreshold, Settings.initialColSize);
      }
      else {
        this.valuesWriter = new DeltaByteArrayWriter(Settings.initialColSize);
      }
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      valuesWriter.writeBytes(Binary.fromByteArray(((String) value).getBytes()));
      statistics.add(value);
    }
  }

  public static class ObjectColumnWriter extends ColumnWriter {
    public ObjectColumnWriter(DataOutput output) {
      super(output);
      this.valuesWriter = new ObjectValuesWriter();
    }

    @Override
    public void write(Object value) throws PicklePotException {
      numValues++;
      ((ObjectValuesWriter)valuesWriter).writeObject(value);
      statistics.add(value);
    }
  }

  public static class ArrayColumnWriter extends ColumnWriter {
    private ColumnWriter lengthWriter;
    private ColumnWriter compWriter;

    public ArrayColumnWriter(DataOutput output, Class clazz) {
      super(output);
      this.lengthWriter = new IntColumnWriter(output);
      Class compClazz = clazz.getComponentType();
      switch (Type.typeOf(compClazz)) {
        case STRING:
          compWriter = new StringColumnWriter(output);
          break;
        case INT:
          compWriter = new IntColumnWriter(output);
          break;
        case LONG:
          compWriter = new LongColumnWriter(output);
          break;
        case FLOAT:
          compWriter = new FloatColumnWriter(output);
          break;
        case DOUBLE:
          compWriter = new DoubleColumnWriter(output);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    @Override
    public void write(Object value) throws PicklePotException {
      int len = Array.getLength(value);
      lengthWriter.write(len);
      for(int i=0; i<len; i++) {
        compWriter.write(Array.get(value, i));
      }
    }

    @Override
    public void writeToBlock() {
      lengthWriter.writeToBlock();
      compWriter.writeToBlock();
    }

    @Override
    public String toString() {
      return "Array(" + lengthWriter.toString() + " " + compWriter.toString() + ")";
    }
  }
}
