package com.intel.picklepot.column.legacy.metadata;

import java.io.Serializable;
import java.lang.reflect.Type;

public class FieldInfo implements Serializable {
  private String fieldName;
  private Type fieldType;

  public FieldInfo(String fieldName, Type fieldType) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Class getFieldClass() {
    return (Class) fieldType;
  }
}
