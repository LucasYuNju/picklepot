package com.intel.picklepot.column;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;
import com.intel.picklepot.serialization.Type;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;

public class ArrayColumnWriter extends ColumnWriter{
  private ValuesWriter lengthWriter;
  private ColumnWriter compWriter;

  public ArrayColumnWriter(DataOutput output, Class clazz) {
    super(output);
    Class compClazz = clazz.getComponentType();
    this.lengthWriter = new DeltaBinaryPackingValuesWriter(Settings.initialColSize);
    switch (Type.get(compClazz)) {
      case STRING:
        compWriter = new StringColumnWriter(output);
        break;
      case INT:
        compWriter = new IntColumnWriter(output);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public void write(Object value) throws PicklePotException {
    Object[] array = (Object[]) value;
    valuesWriter.writeInteger(array.length);
    for(Object component : array) {
      compWriter.write(component);
    }
  }
}
