package com.intel.picklepot.serialization;

import com.intel.picklepot.column.Readers;
import com.intel.picklepot.column.Writers;
import com.intel.picklepot.exception.PicklePotException;

public class UnsafeFloatField extends UnsafeField {

  public UnsafeFloatField(Class clazz, long offset) {
    super(clazz, offset);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = new Writers.FloatColumnWriter(picklePot.getOutput());
    }
    Float floatVal;
    if(clazz == float.class) {
      floatVal = Utils.unsafe().getFloat(object, offset);
    }
    else {
      floatVal = (Float) Utils.unsafe().getObject(object, offset);
    }
    writer.write(floatVal);
  }

  @Override
  public void read(Object object) throws PicklePotException {
    if(reader == null) {
      reader = new Readers.FloatColumnReader(picklePot.getInput());
    }
    Float floatVal = (Float) reader.read();
    if(clazz == float.class) {
      Utils.unsafe().putFloat(object, offset, floatVal);
    }
    else {
      Utils.unsafe().putObject(object, offset, floatVal);
    }
  }
}
