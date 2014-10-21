package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleObjectInspector<T> implements ObjectInspector<T> {
  private Class<T> className;
  private Field[] fields;

  public SimpleObjectInspector(Class<T> className) {
    this.className = className;
    fields = className.getDeclaredFields();
  }

  public void inspect(T obj, List<List<Object>> fieldCube) throws PicklePotException {
    Iterator<List<Object>> iterator = fieldCube.iterator();
    for (Field field : fields) {
      List<Object> valueList = iterator.next();
      String fieldName = field.getName();
      try {
        if (field.isAccessible()) {
          valueList.add(field.get(obj));
        } else {
          try {
            field.setAccessible(true);
            valueList.add(field.get(obj));
          } finally {
            field.setAccessible(false);
          }
        }
      } catch (IllegalAccessException e) {
        throw new PicklePotException("Failed to get field[" + fieldName + "] value.", e);
      }
    }
  }
}
