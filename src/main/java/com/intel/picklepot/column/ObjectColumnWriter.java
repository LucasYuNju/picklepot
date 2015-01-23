package com.intel.picklepot.column;

import com.intel.picklepot.column.values.ObjectValuesWriter;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;

public class ObjectColumnWriter extends ColumnWriter {

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
