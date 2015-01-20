package com.intel.picklepot.unsafe;

import java.io.Serializable;

public class FieldGroup implements Serializable {
  private Class clazz;
  private UnsafeField[] unsafeFields;

  public FieldGroup(Object object) {

  }
}
