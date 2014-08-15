package com.intel.picklepot.storage;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

public class Storage {
  private FieldData[] fieldDatas;

  public Storage(Class cls) {
    int fieldNumber = cls.getFields().length;
    fieldDatas = new FieldData[fieldNumber];
    for (int i=0; i<fieldNumber; i++) {
      fieldDatas[i] = new FieldData(cls.getFields()[i].getType());
    }
  }

  public void add(Object ins) {
    Class cls = ins.getClass();
    Field[] fields = cls.getFields();
    for (int i=0; i<fields.length; i++) {
      Field field = fields[i];
      FieldData fieldData = fieldDatas[i];
      fieldData.addValue(field, ins);
    }
  }

  public void flush() {

  }

  static class FieldData {
    private ValueParser parser;
    private List data;

    public FieldData(Class type) {
      this.parser = ValueParserFactory.getValueParser(type);
      this.data = new LinkedList();
    }

    public List getData() {
      return data;
    }

    public void addValue(Field field, Object obj) {
      try {
        data.add(parser.parse(field, obj));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
  }
}