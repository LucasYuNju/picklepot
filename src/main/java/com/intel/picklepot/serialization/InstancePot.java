package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;
import com.intel.picklepot.metadata.FieldInfo;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InstancePot<T> {

  private ClassInfo<T> classInfo;
  private ObjectInspector inspector;
  private Map<String, List<?>> fieldsMap = new HashMap<String, List<?>>();
  private List<List<?>> fieldsList = new LinkedList<List<?>>();

  public InstancePot(Class<T> aClass) {
    this.classInfo = initiate(aClass);
    this.inspector = new SimpleObjectInspector(aClass);
  }

  private ClassInfo<T> initiate(Class<T> aClass) {
    classInfo = new ClassInfo<T>(aClass);
    Field[] fields = aClass.getDeclaredFields();
    for (Field field : fields) {
      Class<?> fieldType = field.getType();
      FieldInfo fieldInfo = new FieldInfo(field.getName(), fieldType);
      classInfo.putFieldInfo(fieldInfo);
      if (fieldType.equals(String.class)) {
        List<String> objects = new LinkedList<String>();
        fieldsMap.put(field.getName(), objects);
        fieldsList.add(objects);
      } else if (fieldType.equals(Integer.class) || fieldType.getName().equals("int")) {
        List<Integer> objects = new LinkedList<Integer>();
        fieldsMap.put(field.getName(), objects);
        fieldsList.add(objects);
      } else {
        throw new RuntimeException("do not support field type:" + fieldType);
      }
    }
    return classInfo;
  }

  public void addObjectValue(Object obj) throws PicklePotException {
    // add field value into value list directly, as fields order in inspector should be same as
    // fieldsList order.
    this.inspector.inspect(obj, fieldsList);
  }

  public List<?> getFieldValues(String fieldName) {
    return fieldsMap.get(fieldName);
  }

  public ClassInfo<T> getClassInfo() {
    return classInfo;
  }

}
