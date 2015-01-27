package com.intel.picklepot.serialization;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.exception.PicklePotException;

/**
 * UnsafeObjectField can serialize object in this field with Java built-in serializer,
 * or unfold object and call FieldGroup to handle it.
 */
public class UnsafeNestedField extends UnsafeField {
  private FieldGroup group;

  public UnsafeNestedField(Object object, long offset) {
    super(object.getClass(), offset);
    group = new FieldGroup(object);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    Object fieldObj = Utils.unsafe().getObject(object, offset);
    group.write(fieldObj);
  }

  @Override
  public void read(Object object) throws  PicklePotException{
    Object fieldObj = picklePot.instantiate(clazz);
    group.read(fieldObj);
    Utils.unsafe().putObject(object, offset, fieldObj);
  }

  @Override
  public void flush() {
    group.flush();
  }

  @Override
  public void setPicklePot(PicklePotImpl picklePot) {
    super.setPicklePot(picklePot);
    group.setPicklePot(picklePot);
  }
}
