package com.intel.picklepot.unsafe;

import com.intel.picklepot.columnar.Utils;

public class UnsafeUnsupportedField extends UnsafeField {

  public UnsafeUnsupportedField(Class clazz, long offset, NewPicklePotImpl picklepot) {
    super(clazz, offset, picklepot);
  }

  @Override
  public void write(Object object) {
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
