package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ColumnReader;
import com.intel.picklepot.column.ColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class UnsafeField implements Serializable{
  protected transient Class clazz;
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
    String prefix = Type.typeOf(clazz) + "_" + clazz.getName() + " ";
    if(writer != null) {
      return prefix + writer.toString();
    }
    if(reader != null) {
      return prefix + reader.toString();
    }
    return prefix;
  }

  public void updateOffset(long offset) throws PicklePotException {
    this.offset = offset;
  }

  private void writeObject(ObjectOutputStream stream) throws IOException {
    stream.defaultWriteObject();
    stream.writeObject(clazz.getName());
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    String className = (String) stream.readObject();
    this.clazz = Utils.resolveClass(className);
  }
}
