package com.intel.picklepot.unsafe;

import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

/**
 * UnsafeObjectField can serialize object in this field with Java built-in serializer,
 * or unfold object and call FieldGroup to handle it.
 */
public class UnsafeNestedField extends UnsafeField {
  private FieldGroup group;

  public UnsafeNestedField(Object object, long offset, NewPicklePotImpl picklepot) {
    super(object.getClass(), offset, picklepot);
    group = new FieldGroup(object, picklePot);
  }

  @Override
  public void write(Object object) throws PicklePotException {
    Object fieldObj = Utils.getUnsafe().getObject(object, offset);
    group.write(fieldObj);
  }

  @Override
  public Object read(Object object) {
    Object fieldObj = picklePot.instantiate(clazz);
    group.read(fieldObj);
    Utils.getUnsafe().putObject(object, offset, fieldObj);
    return null;
  }

  @Override
  public void flush() {
    group.flush();
  }

  @Override
  public void setPicklePot(NewPicklePotImpl picklePot) {
    super.setPicklePot(picklePot);
    group.setPicklePot(picklePot);
  }
}
