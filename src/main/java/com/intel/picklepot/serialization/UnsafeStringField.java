package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeStringField extends UnsafeField{

  public UnsafeStringField(Class clazz, long offset, PicklePotImpl picklepot) {
    super(clazz, offset, picklepot);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = Utils.getColumnWriter(clazz, picklePot.getOutput());
    }
    String strVal = (String) Utils.getUnsafe().getObject(object, offset);
    writer.write(strVal);
  }

  @Override
  public Object read(Object object) {
    if(reader == null) {
      reader = Utils.getColumnReader(clazz, picklePot.getInput());
    }
    String strVal = (String) reader.read();
    Utils.getUnsafe().putObject(object, offset, strVal);
    return object;
  }
}
