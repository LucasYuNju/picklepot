package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ColumnReader;
import com.intel.picklepot.column.ColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

import java.io.Serializable;

public abstract class UnsafeField implements Serializable{
  protected Class clazz;
  protected transient long offset;
  protected transient ColumnWriter writer;
  protected transient ColumnReader reader;
  protected transient PicklePotImpl picklePot;

  /**
   * @param clazz field type
   * @param offset field offset
   */
  public UnsafeField(Class clazz, long offset) {
    this.clazz = clazz;
    this.offset = offset;
  }

  public void write(Object object) throws PicklePotException {
    if(writer == null) {
      throw new PicklePotException("writer not initialized");
    }
    Object fieldObj = Utils.unsafe().getObject(object, offset);
    writer.write(fieldObj);
  }

  public void read(Object object) throws PicklePotException{
    if(reader == null) {
      throw new PicklePotException("reader not initialized");
    }
    Object fieldObj = reader.read();
    Utils.unsafe().putObject(object, offset, fieldObj);
  }

  public void flush() {
    writer.writeToBlock();
  }

  public void setPicklePot(PicklePotImpl picklePot) {
    this.picklePot = picklePot;
  }

  @Override
  public String toString() {
    return Type.get(clazz) + ":" + clazz.getName();
  }

  public void updateOffset(long offset) throws PicklePotException {
    this.offset = offset;
  }
}
