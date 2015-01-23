package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.IntColumnReader;
import com.intel.picklepot.column.IntColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeIntField extends UnsafeField{

  public UnsafeIntField(Class clazz, long offset, PicklePotImpl picklePot, boolean directAccess) {
    super(clazz, offset, picklePot, directAccess);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new IntColumnWriter(picklePot.getOutput());
    }
    if(directAccess) {
      writer.write(object);
      return;
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
      reader = new IntColumnReader(picklePot.getInput());
    }
    if(directAccess) {
      return reader.read();
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
