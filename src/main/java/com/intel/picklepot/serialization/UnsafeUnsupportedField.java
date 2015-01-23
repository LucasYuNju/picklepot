package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeUnsupportedField extends UnsafeField {

  public UnsafeUnsupportedField(Class clazz, long offset, PicklePotImpl picklepot) {
    super(clazz, offset, picklepot);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = Utils.getColumnWriter(clazz, picklePot.getOutput());
    }
    Object writeVal = Utils.getUnsafe().getObject(object, offset);
    writer.write(writeVal);
  }

  @Override
  public Object read(Object object) {
    if(reader == null) {
      reader = Utils.getColumnReader(clazz, picklePot.getInput());
    }
    Utils.getUnsafe().putObject(object, offset, reader.read());
    return object;
  }
}
