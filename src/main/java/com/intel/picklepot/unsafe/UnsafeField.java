package com.intel.picklepot.unsafe;

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
  protected transient NewPicklePotImpl picklePot;

  public UnsafeField(Class clazz, long offset, NewPicklePotImpl picklepot) {
    this.clazz = clazz;
    this.offset = offset;
    this.picklePot = picklepot;
  }

  public void setPicklePot(NewPicklePotImpl picklePot) {
    this.picklePot = picklePot;
  }

  public static UnsafeField getUnsafeField(Class clazz, Object object, long offset, NewPicklePotImpl picklePot) {
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

  public static UnsafeField getUnsafeField(Class clazz, NewPicklePotImpl picklePot) {
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
