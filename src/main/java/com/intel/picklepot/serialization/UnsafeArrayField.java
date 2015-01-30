package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeArrayField extends UnsafeField{

  public UnsafeArrayField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.ArrayColumnWriter(picklePot.getOutput(), clazz);
    }
    Object writeVal = Utils.unsafe().getObject(object, offset);
    writer.write(writeVal);
  }

  @Override
  public void read(Object object) throws PicklePotException {
    if(reader == null) {
      this.reader = new Readers.ArrayColumnReader(picklePot.getInput(), clazz);
    }
    Utils.unsafe().putObject(object, offset, reader.read());
  }
}
