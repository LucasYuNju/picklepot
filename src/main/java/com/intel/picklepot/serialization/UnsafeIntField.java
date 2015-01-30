package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeIntField extends UnsafeField{

  public UnsafeIntField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.IntColumnWriter(picklePot.getOutput());
    }
    Integer intVal;
    if(clazz == int.class) {
      intVal = Utils.unsafe().getInt(object, offset);
    }
    else {
      intVal = (Integer) Utils.unsafe().getObject(object, offset);
    }
    writer.write(intVal);
  }

  @Override
  public void read(Object object) throws PicklePotException {
    if(reader == null) {
      reader = new Readers.IntColumnReader(picklePot.getInput());
    }
    Integer intVal = (Integer) reader.read();
    if(clazz == int.class) {
      Utils.unsafe().putInt(object, offset, intVal);
    }
    else {
      Utils.unsafe().putObject(object, offset, intVal);
    }
  }
}
