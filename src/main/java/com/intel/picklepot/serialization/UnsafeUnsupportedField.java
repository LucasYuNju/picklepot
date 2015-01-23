package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ObjectColumnReader;
import com.intel.picklepot.column.ObjectColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeUnsupportedField extends UnsafeField {

  public UnsafeUnsupportedField(Class clazz, long offset, PicklePotImpl picklepot, boolean directAccess) {
    super(clazz, offset, picklepot, directAccess);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new ObjectColumnWriter(picklePot.getOutput());
    }
    if(directAccess) {
      writer.write(object);
      return;
    }
    Object writeVal = Utils.getUnsafe().getObject(object, offset);
    writer.write(writeVal);
  }

  @Override
  public Object read(Object object) {
    if(reader == null) {
      reader = new ObjectColumnReader(picklePot.getInput());
    }
    if(directAccess) {
      return reader.read();
    }
    Utils.getUnsafe().putObject(object, offset, reader.read());
    return object;
  }
}
