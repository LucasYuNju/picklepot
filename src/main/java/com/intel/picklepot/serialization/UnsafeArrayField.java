package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ArrayColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeArrayField extends UnsafeField{

  public UnsafeArrayField(Class clazz, long offset, PicklePotImpl picklepot, boolean directAccess) {
    super(clazz, offset, picklepot, directAccess);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new ArrayColumnWriter(picklePot.getOutput(), object.getClass());
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public Object read(Object object) {
    throw new UnsupportedOperationException();
  }
}
