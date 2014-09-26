package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class SimpleObjectInspector<T> implements ObjectInspector<T> {
  private Class<T> className;

  public SimpleObjectInspector(Class<T> className) {
    this.className = className;
  }

  public Map<String, Object> inspect(T obj) throws PicklePotException {
    Field[] fields = className.getDeclaredFields();
    Map<String, Object> fieldMapping = new HashMap<String, Object>(fields.length);
    for (Field field : fields) {
      String fieldName = field.getName();
      try {
        if (field.isAccessible()) {
          fieldMapping.put(fieldName, field.get(obj));
        } else {
          try {
            field.setAccessible(true);
            fieldMapping.put(fieldName, field.get(obj));
          } finally {
            field.setAccessible(false);
          }
        }
      } catch (IllegalAccessException e) {
        throw new PicklePotException("Failed to get field[" + fieldName + "] value.", e);
      }
    }
    return fieldMapping;
  }
}
