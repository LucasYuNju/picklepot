package com.intel.picklepot.columnar.legacy.serialization;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.columnar.legacy.metadata.ClassInfo;
import com.intel.picklepot.columnar.legacy.metadata.FieldInfo;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InstancePot<T> {

  private ClassInfo<T> classInfo;
  private ObjectInspector<Object> inspector;
  private Map<String, List<?>> fieldsMap = new HashMap<String, List<?>>();
  private List<List<?>> fieldsList = new LinkedList<List<?>>();
  private boolean isUnsuppoted = false;
  private boolean initialized = false;


  public InstancePot(Class<T> aClass) {
//    this.classInfo = initiate(aClass);
//    this.inspector = new SimpleObjectInspector(aClass);
  }

  private ClassInfo<T> initiate(Class<T> aClass, Object t) {
    classInfo = new ClassInfo<T>(aClass);
    if(isUnsupportedInstance(aClass, t)) {
      classInfo.serializWithJava();
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
      Class<?> fieldType = null;

      boolean accessible = field.isAccessible();
      field.setAccessible(true);
      try {
        fieldType = field.get(t).getClass();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      field.setAccessible(accessible);

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

  public void addObjectValue(T obj) throws PicklePotException {
    if(!initialized) {
      this.classInfo = initiate((Class<T>)obj.getClass(), obj);
      this.inspector = new SimpleObjectInspector(obj.getClass());
      initialized = true;
    }
    if(isUnsuppoted) {
      List<Object> list = (List<Object>) fieldsList.get(0);
      list.add(obj);
    }
    else {
      List lists = fieldsList;
      this.inspector.inspect(obj, lists);
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
   * return true when aClass=String, so that ColumnWriter can compress objects as a single column
   * 
   * TODO improper method name
   */
  public static boolean isUnsupportedInstance(Class aClass, Object obj) {
    Field[] fields = aClass.getDeclaredFields();
    if(aClass.isArray())
      return true;
    for (Field field : fields) {
      boolean accessible = field.isAccessible();
      field.setAccessible(true);
      try {
        Class<?> fieldType = field.get(obj).getClass();
        if(!fieldType.equals(String.class)
            && !fieldType.equals(Integer.class)
            && !fieldType.equals(Integer.TYPE)) {
          return true;
        }
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
      field.setAccessible(accessible);
    }
    return false;
  }
}
