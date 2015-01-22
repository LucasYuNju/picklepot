package com.intel.picklepot.unsafe;

import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeIntField extends UnsafeField{

  public UnsafeIntField(Class clazz, long offset, NewPicklePotImpl picklePot) {
    super(clazz, offset, picklePot);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = Utils.getColumnWriter(clazz, picklePot.getOutput());
    }
    Integer intVal;
    if(clazz == int.class) {
      intVal = Utils.getUnsafe().getInt(object, offset);
    }
    else {
      intVal = (Integer) Utils.getUnsafe().getObject(object, offset);
    }
    writer.write(intVal);
  }

  @Override
  public Object read(Object object) {
    if(reader == null) {
      reader = Utils.getColumnReader(clazz, picklePot.getInput());
    }
    Integer intVal = (Integer) reader.read();
    if(clazz == int.class) {
      Utils.getUnsafe().putInt(object, offset, intVal);
    }
    else {
      Utils.getUnsafe().putObject(object, offset, intVal);
    }
    return object;
  }
}
