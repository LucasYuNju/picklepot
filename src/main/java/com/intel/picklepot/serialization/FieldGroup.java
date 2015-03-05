package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.exception.PicklePotException;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * FieldGroup distribute serialization work to UnsafeFields.
 * If clazz is not Type.NESTED(e.g., String), FieldGroup does not inspect the Fields inside clazz.
 */
public class FieldGroup implements Serializable {
  private transient Object ret;
  private transient Class clazz;
  private UnsafeField[] unsafeFields;
  //record how many objs has been serialized
  private long numVals;

  /**
   * Field offset varies on different JVM, it's necessary to update offset after FieldGroup is deserialized
   */
  public void updateOffset() throws PicklePotException {
    if(Type.typeOf(clazz) == Type.NESTED) {
      Field[] fields = clazz.getDeclaredFields();
      int i = 0;
      for (Field field : fields) {
        if (Modifier.isStatic(field.getModifiers()))
          continue;
        long offset = Utils.unsafe().objectFieldOffset(field);
        unsafeFields[i++].updateOffset(offset);
      }
    }
    else {
      try {
        Field retField = FieldGroup.class.getDeclaredField("ret");
        long offset = Utils.unsafe().objectFieldOffset(retField);
        unsafeFields[0].updateOffset(offset);
      } catch (NoSuchFieldException e) {
        throw new PicklePotException(e);
      }
    }
  }

  public FieldGroup(Object object) {
    this.clazz = object.getClass();
    if(Type.typeOf(clazz) == Type.NESTED) {
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

        long offset = Utils.unsafe().objectFieldOffset(field);
        //considering generic field and autoboxing, one of fieldObj.getClass() and field.getType() is the real class.
        Class fieldClass = field.getType() == Object.class ? fieldObj.getClass() : field.getType();
        unsafeFieldList.add(UnsafeFieldFactory.getUnsafeField(fieldClass, fieldObj, offset));
      }
      unsafeFields = new UnsafeField[unsafeFieldList.size()];
      for(int i=0; i<unsafeFields.length; i++) {
        unsafeFields[i] = unsafeFieldList.get(i);
      }
    }
    else {
      try {
        Field retField = FieldGroup.class.getDeclaredField("ret");
        long offset = Utils.unsafe().objectFieldOffset(retField);
        unsafeFields = new UnsafeField[] {UnsafeFieldFactory.getUnsafeField(object.getClass(), object, offset)};
      } catch (NoSuchFieldException e) {
        e.printStackTrace();
      }
    }
  }

  public void write(Object object) throws PicklePotException {
    if(isNested()) {
      for (UnsafeField field : unsafeFields) {
        field.write(object);
      }
    }
    else {
      ret = object;
      unsafeFields[0].write(this);
    }
  }

  public void read(Object object) throws PicklePotException{
    for(UnsafeField field : unsafeFields) {
      field.read(object);
    }
  }

  public Object read() throws PicklePotException{
    unsafeFields[0].read(this);
    return ret;
  }

  public void flush() {
    for(UnsafeField field : unsafeFields) {
      field.flush();
    }
  }

  public boolean isNested() {
    return Type.typeOf(clazz) == Type.NESTED;
  }

  public Class getClazz() {
    return clazz;
  }

  /**
   * need to be called after FieldGroup is read from ObjectInputStream.
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[");
    if(!isNested()) {
      builder.append("N*");
    }
    for(int i=0; i<unsafeFields.length; i++) {
      if(i != 0) {
        builder.append(", ");
      }
      builder.append(unsafeFields[i].toString());
    }
    builder.append("]");
    return builder.toString();
  }

  private void writeObject(ObjectOutputStream stream) throws IOException{
    stream.defaultWriteObject();
    stream.writeObject(clazz.getName());
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    String className = (String) stream.readObject();
    this.clazz = Utils.resolveClass(className);
  }
}
