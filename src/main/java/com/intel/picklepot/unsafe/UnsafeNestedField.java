package com.intel.picklepot.unsafe;

import com.intel.picklepot.columnar.Utils;
import com.intel.picklepot.exception.PicklePotException;

/**
 * UnsafeObjectField can serialize object in this field with Java built-in serializer,
 * or unfold object and call FieldGroup to handle it.
 */
public class UnsafeNestedField extends UnsafeField{
  private FieldGroup group;

  public UnsafeNestedField(Object object, long offset, NewPicklePotImpl picklepot) {
    super(object.getClass(), offset, picklepot);
    if(Utils.toFieldType(clazz) == FieldType.NESTED) {
      group = new FieldGroup(object, picklePot);
    }
  }

  @Override
  public void write(Object object) throws PicklePotException {
    if(group != null) {
      group.write(object);
    }
    else {
      if(writer == null) {
        writer = Utils.getColumnWriter(clazz, picklePot.getOutput());
      }
      Object writeVal = Utils.getUnsafe().getObject(object, offset);
      writer.write(writeVal);
    }
  }

  @Override
  public Object read(Object object) {
    if(group != null) {
      group.read(object);
    }
    else {
      if(reader == null) {
        reader = Utils.getColumnReader(clazz, picklePot.getInput());
      }
      Utils.getUnsafe().putObject(object, offset, reader.read());
    }
    return object;
  }
}
