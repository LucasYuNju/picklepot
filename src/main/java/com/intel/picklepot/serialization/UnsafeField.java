package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.column.ColumnReader;
import com.intel.picklepot.column.ColumnWriter;
import com.intel.picklepot.exception.PicklePotException;

import java.io.Serializable;

public abstract class UnsafeField implements Serializable{
  protected Class clazz;
  protected long offset;
  protected boolean directAccess;
  protected transient ColumnWriter writer;
  protected transient ColumnReader reader;
  protected transient PicklePotImpl picklePot;

  /**
   * @param clazz
   * @param offset
   * @param picklepot
   * @param directAccess if true, serialize reveived object itself, rather than object's field
   */
  public UnsafeField(Class clazz, long offset, PicklePotImpl picklepot, boolean directAccess) {
    this.clazz = clazz;
    this.offset = offset;
    this.picklePot = picklepot;
    this.directAccess = directAccess;
  }

  /**
   * @param object
   * write object itself into outputstream. This is a special implementation.
   */
  public abstract void write(Object object) throws PicklePotException;

  /**
   * @param object not used
   * @return deserialized field object
   */
  public abstract Object read(Object object);

  public void flush() {
    writer.writeToBlock();
  }

  public void setPicklePot(PicklePotImpl picklePot) {
    this.picklePot = picklePot;
  }
}
