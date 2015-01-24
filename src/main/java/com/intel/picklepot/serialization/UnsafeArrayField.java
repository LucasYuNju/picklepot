package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ArrayColumnReader;
import com.intel.picklepot.column.ArrayColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeArrayField extends UnsafeField{

  public UnsafeArrayField(Class clazz, long offset, PicklePotImpl picklepot) {
    super(clazz, offset, picklepot);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new ArrayColumnWriter(picklePot.getOutput(), clazz);
    }
    Object writeVal = Utils.getUnsafe().getObject(object, offset);
    writer.write(writeVal);
  }

  @Override
  public Object read(Object object) {
    if(reader == null) {
      this.reader = new ArrayColumnReader(picklePot.getInput(), clazz);
    }
    Utils.getUnsafe().putObject(object, offset, reader.read());
    return object;
  }
}
