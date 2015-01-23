package com.intel.picklepot.column.legacy.metadata;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class ClassInfo<T> implements Serializable {
  private Class<T> classIns;
  private Map<String, FieldInfo> fieldInfos = new LinkedHashMap<String, FieldInfo>();
  private boolean useJava = false;

  public ClassInfo(Class<T> aClass) {
    this.classIns = aClass;
  }

  public void putFieldInfo(FieldInfo fieldInfo) {
    fieldInfos.put(fieldInfo.getFieldName(), fieldInfo);
  }

  public FieldInfo getFieldInfo(String fieldName) {
    return fieldInfos.get(fieldName);
  }

  public Map<String, FieldInfo> getFieldInfos() {
    return fieldInfos;
  }

  public Class<T> getClassIns() {
    return classIns;
  }

  public void serializWithJava() {
    useJava = true;
  }

  public boolean isSerializedWithJava() {
    return useJava;
  }
}
