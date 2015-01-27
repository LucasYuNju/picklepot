package com.intel.picklepot.column;

import parquet.column.values.ValuesReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class ObjectValuesReader extends ValuesReader{
  private ObjectInputStream ois;

  @Override
  public void initFromPage(int i, byte[] bytes, int i2) throws IOException {
    ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
  }

  @Override
  public void skip() {

  }

  public Object readObject() {
    try {
      return ois.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }
}
