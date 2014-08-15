package com.intel.picklepot.storage;

public class ValueParserFactory {
  public static ValueParser getValueParser(Class cls) {
    if (cls.equals(Integer.TYPE)) {
      return new IntValueParser();
    }
    return null;
  }
}
