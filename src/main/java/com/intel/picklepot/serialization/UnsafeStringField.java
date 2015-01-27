package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeStringField extends UnsafeField{

  public UnsafeStringField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.StringColumnWriter(picklePot.getOutput());
    }
    super.write(object);
  }

  @Override
  public void read(Object object) throws PicklePotException{
    if(reader == null) {
      reader = new Readers.StringColumnReader(picklePot.getInput());
    }
    super.read(object);
  }
}
