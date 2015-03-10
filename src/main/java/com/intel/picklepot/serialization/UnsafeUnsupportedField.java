package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeUnsupportedField extends UnsafeField {

  public UnsafeUnsupportedField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.ObjectColumnWriter(picklePot.getOutput());
    }
//    if(clazz.isPrimitive()) {
//      throw new PicklePotException("class:" + clazz.getName() + " not supported");
//    }
    super.write(object);
  }

  @Override
  public void read(Object object) throws PicklePotException{
    if(reader == null) {
      reader = new Readers.ObjectColumnReader(picklePot.getInput());
    }
    super.read(object);
  }
}
