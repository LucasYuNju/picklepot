package com.intel.picklepot.metadata;

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

  public Type getFieldType() {
    return fieldType;
  }

  public Class getFieldClass() {
    String className = fieldType.toString().replace("class", "").trim();
    if(className.equals(String.class.getName()))
      return String.class;
    return Integer.class;
  }
}
