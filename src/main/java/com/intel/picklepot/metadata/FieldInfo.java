package com.intel.picklepot.metadata;

import java.lang.reflect.Type;

public class FieldInfo {
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
}
