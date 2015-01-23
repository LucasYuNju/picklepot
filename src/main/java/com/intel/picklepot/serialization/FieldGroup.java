package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldGroup delgate actual serialization work to UnsafeFields.
 * If clazz is String(or any other supported Type), FieldGroup does not inspect the Fields inside String.
 * Instead, FieldGroup will work as if clazz contains a single String Field, and serialize Strings with UnsafeStringField.
 */
public class FieldGroup implements Serializable {
  private Class clazz;
  private UnsafeField[] unsafeFields;
  //a temporary way to record how many objs has been serialized
  private long numVals;

  public FieldGroup(Object object, PicklePotImpl picklePot) {
    this.clazz = object.getClass();
    if(Utils.toFieldType(clazz) == FieldType.NESTED) {
      Field[] fields = clazz.getDeclaredFields();
      List<UnsafeField> unsafeFieldList = new ArrayList<UnsafeField>();
      for (Field field : fields) {
        if (Modifier.isStatic(field.getModifiers()))
          continue;
        Object fieldObj = 0;
        boolean accessible = field.isAccessible();
        field.setAccessible(true);
        try {
          fieldObj = field.get(object);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        field.setAccessible(accessible);

        long offset = Utils.getUnsafe().objectFieldOffset(field);
        unsafeFieldList.add(UnsafeField.getUnsafeField(field.getType(), fieldObj, offset, picklePot));
      }
      unsafeFields = new UnsafeField[unsafeFieldList.size()];
      for(int i=0; i<unsafeFields.length; i++) {
        unsafeFields[i] = unsafeFieldList.get(i);
      }
    }
    else {
      unsafeFields = new UnsafeField[] {UnsafeField.getUnsafeField(object.getClass(), picklePot)};
    }
  }

  public Object write(Object object) throws PicklePotException {
    for (UnsafeField field : unsafeFields) {
      field.write(object);
    }
    return object;
  }

  public void read(Object object) {
    for(UnsafeField field : unsafeFields) {
      field.read(object);
    }
  }

  public Object read() {
    return unsafeFields[0].read(null);
  }

  public void flush() {
    for(UnsafeField field : unsafeFields) {
      field.flush();
    }
  }

  public boolean isNested() {
    return Utils.toFieldType(clazz) == FieldType.NESTED;
  }

  public Class getClazz() {
    return clazz;
  }

  /**
   * need to be called when FieldGroup is read from ObjectInputStream,
   *
   * @param picklePot
   */
  public void setPicklePot(PicklePotImpl picklePot) {
    for(UnsafeField field : unsafeFields) {
      field.setPicklePot(picklePot);
    }
  }

  public long getNumVals() {
    return numVals;
  }

  public void setNumvals(long numvals) {
    this.numVals = numvals;
  }
}