package com.intel.picklepot.column.legacy.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

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
        String fieldName = field.getName();
        throw new PicklePotException("Failed to get field[" + fieldName + "] value.", e);
      }
    }
  }
}
