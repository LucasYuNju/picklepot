package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeDoubleField extends UnsafeField {

  public UnsafeDoubleField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.DoubleColumnWriter(picklePot.getOutput());
    }
    Double doubleVal;
    if(clazz == double.class) {
      doubleVal = Utils.unsafe().getDouble(object, offset);
    }
    else {
      doubleVal = (Double) Utils.unsafe().getObject(object, offset);
    }
    writer.write(doubleVal);
  }

  @Override
  public void read(Object object) throws PicklePotException {
    if(reader == null) {
      reader = new Readers.DoubleColumnReader(picklePot.getInput());
    }
    Double doubleVal = (Double) reader.read();
    if(clazz == double.class) {
      Utils.unsafe().putDouble(object, offset, doubleVal);
    }
    else {
      Utils.unsafe().putObject(object, offset, doubleVal);
    }
  }
}