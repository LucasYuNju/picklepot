package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;
import com.intel.picklepot.metadata.FieldInfo;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InstancePot<T> {

  private ClassInfo<T> classInfo;
  private ObjectInspector inspector;
  private Map<String, List<Object>> fieldsValue = new HashMap<String, List<Object>>();

  public InstancePot(Class<T> aClass) {
    this.classInfo = initiate(aClass);
    this.inspector = new SimpleObjectInspector(aClass);
  }

  private ClassInfo<T> initiate(Class<T> aClass) {
    classInfo = new ClassInfo<T>(aClass);
    Field[] fields = aClass.getFields();
    for (Field field : fields) {
      FieldInfo fieldInfo = new FieldInfo(field.getName(), field.getType());
      classInfo.putFieldInfo(fieldInfo);
    }
    return classInfo;
  }

  public void addObjectValue(Object obj) throws PicklePotException {
    Map<String, Object> objectValue = this.inspector.inspect(obj);
    for (String key : objectValue.keySet()) {
      Object value = objectValue.get(key);
      List<Object> fieldValues = fieldsValue.get(key);
      if (fieldValues == null) {
        fieldValues = new ArrayList<Object>();
        fieldsValue.put(key, fieldValues);
      }
      fieldValues.add(value);
    }
  }

  public List<Object> getFieldValues(String fieldName) {
    return fieldsValue.get(fieldName);
  }

  public ClassInfo<T> getClassInfo() {
    return classInfo;
  }

}
