package com.intel.picklepot.storage;

import java.lang.reflect.Field;

public interface ValueParser {
  public Object parse(Field field, Object obj) throws IllegalAccessException;
}

class IntValueParser implements ValueParser{

  @Override
  public Object parse(Field field, Object obj) throws IllegalAccessException {
    return field.getInt(obj);
  }
}

class LongValueParser implements ValueParser {

  @Override
  public Object parse(Field field, Object obj) throws IllegalAccessException {
    return field.getLong(obj);
  }
}