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
  private boolean isUnsuppoted = false;

  public InstancePot(Class<T> aClass) {
    this.classInfo = initiate(aClass);
    this.inspector = new SimpleObjectInspector(aClass);
  }

  private ClassInfo<T> initiate(Class<T> aClass) {
    classInfo = new ClassInfo<T>(aClass);
    if(isUnsupportedInstance(aClass)) {
      isUnsuppoted = true;
      FieldInfo fieldInfo = new FieldInfo(aClass.getName(), aClass);
      classInfo.putFieldInfo(fieldInfo);
      List<Object> objects = new LinkedList<Object>();
      fieldsMap.put(aClass.getName(), objects);
      fieldsList.add(objects);
      return classInfo;
    }
    Field[] fields = aClass.getDeclaredFields();
    for (Field field : fields) {
      Class<?> fieldType = field.getType();
      FieldInfo fieldInfo = new FieldInfo(field.getName(), fieldType);
      classInfo.putFieldInfo(fieldInfo);
      if (fieldType.equals(String.class)) {
        List<String> objects = new LinkedList<String>();
        fieldsMap.put(field.getName(), objects);
        fieldsList.add(objects);
      } else if (fieldType.equals(Integer.class) || fieldType.equals(Integer.TYPE)) {
        List<Integer> objects = new LinkedList<Integer>();
        fieldsMap.put(field.getName(), objects);
        fieldsList.add(objects);
      } else {
        //non-integer and non-string fields are encoded with java serializer
        List<Object> objects = new LinkedList<Object>();
        fieldsMap.put(field.getName(), objects);
        fieldsList.add(objects);
      }
    }
    return classInfo;
  }

  public void addObjectValue(Object obj) throws PicklePotException {
    if(isUnsuppoted) {
      List<Object> list = (List<Object>) fieldsList.get(0);
      list.add(obj);
    }
    else {
      // add field value into value list directly, as fields order in inspector should be same as
      // fieldsList order.
      this.inspector.inspect(obj, fieldsList);
    }
  }

  public List<?> getFieldValues(String fieldName) {
    return fieldsMap.get(fieldName);
  }

  public ClassInfo<T> getClassInfo() {
    return classInfo;
  }

  /**
   * @param aClass class of instance
   * @return whether aClass is an ArrayClass or contains unsupported fields(non-string & non-integer)
   */
  public static boolean isUnsupportedInstance(Class aClass) {
    Field[] fields = aClass.getDeclaredFields();
    if(aClass.isArray())
      return true;
    for (Field field : fields) {
      Class<?> fieldType = field.getType();
      if(!fieldType.equals(String.class)
          && !fieldType.equals(Integer.class)
          && !fieldType.equals(Integer.TYPE)) {
        return true;
      }
    }
    return false;
  }
}
