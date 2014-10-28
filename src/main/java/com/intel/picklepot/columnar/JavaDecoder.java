package com.intel.picklepot.columnar;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class JavaDecoder<T> implements Decoder<T>{
  @Override
  public Iterator<T> decode(byte[] bytes, String className) {
    List<T> values = new LinkedList<T>();
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      ObjectInputStream ois = new ObjectInputStream(bais);
      while(bais.available() > 0) {
        T value = (T) ois.readObject();
        values.add(value);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return values.iterator();
  }
}
