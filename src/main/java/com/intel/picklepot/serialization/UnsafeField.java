package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.columnar.ColumnReader;
import com.intel.picklepot.columnar.ColumnWriter;
import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

import java.io.Serializable;

public class UnsafeField implements Serializable{
  protected long offset;
  protected Class clazz;
  protected transient ColumnWriter writer;
  protected transient ColumnReader reader;
  protected transient PicklePotImpl picklePot;

  public UnsafeField(Class clazz, long offset, PicklePotImpl picklepot) {
    this.clazz = clazz;
    this.offset = offset;
    this.picklePot = picklepot;
  }

  public void setPicklePot(PicklePotImpl picklePot) {
    this.picklePot = picklePot;
  }

  public static UnsafeField getUnsafeField(Class clazz, Object object, long offset, PicklePotImpl picklePot) {
    switch (Utils.toFieldType(object.getClass())) {
      case INT:
        return new UnsafeIntField(clazz, offset, picklePot);
      case STRING:
        return new UnsafeStringField(clazz, offset, picklePot);
      case NESTED:
        return new UnsafeNestedField(object, offset, picklePot);
      case UNSUPPRTED:
        return new UnsafeUnsupportedField(clazz, offset, picklePot);
      default:
        return null;
    }
  }

  public static UnsafeField getUnsafeField(Class clazz, PicklePotImpl picklePot) {
    return new UnsafeField(clazz, 0, picklePot);
  }

  /**
   * @param object
   * write object itself into outputstream. This is a special implementation.
   */
  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      writer = Utils.getColumnWriter(clazz, picklePot.getOutput());
    }
    writer.write(object);
  }

  /**
   * @param object not used
   * @return deserialized field object
   */
  public Object read(Object object) {
    if(reader == null) {
      reader = Utils.getColumnReader(clazz, picklePot.getInput());
    }
    return reader.read();
  }

  public void flush() {
    writer.writeToBlock();
  }
}
