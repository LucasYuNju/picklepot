package com.intel.picklepot.unsafe;

import com.intel.picklepot.columnar.ColumnWriter;

import java.io.Serializable;

public abstract class UnsafeField implements Serializable{
  private long offset;
  private Class clazz;
  private FieldGroup group;
  private transient ColumnWriter writer;

  public abstract void write(Object object);

}
