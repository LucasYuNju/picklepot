package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeLongField extends UnsafeField{

  public UnsafeLongField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.LongColumnWriter(picklePot.getOutput());
    }
    Long longVal;
    if(clazz == long.class) {
      longVal = Utils.unsafe().getLong(object, offset);
    }
    else {
      longVal = (Long) Utils.unsafe().getObject(object, offset);
    }
    writer.write(longVal);
  }

  @Override
  public void read(Object object) throws PicklePotException{
    if(reader == null) {
      reader = new Readers.LongColumnReader(picklePot.getInput());
    }
    Long longVal = (Long) reader.read();
    if(clazz == long.class) {
      Utils.unsafe().putLong(object, offset, longVal);
    }
    else {
      Utils.unsafe().putObject(object, offset, longVal);
    }
  }
}
