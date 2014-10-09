package com.intel.picklepot.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ClassInfo<T> implements Serializable {
  private Class<T> classIns;
  private Map<String, FieldInfo> fieldInfos = new HashMap<String, FieldInfo>();

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
}
