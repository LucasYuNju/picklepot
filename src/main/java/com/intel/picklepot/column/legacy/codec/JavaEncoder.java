package com.intel.picklepot.column.legacy.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * This is an alternative to encode non-integer and non-string fields.
 * @param <T>
 */
public class JavaEncoder<T> implements Encoder<T> {
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private ObjectOutputStream oos;

  public JavaEncoder() {
    try {
      oos = new ObjectOutputStream(baos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public OutputStream getOutputStream() {
    return baos;
  }

  @Override
  public void encode(Iterator<T> values, int num) {
    while(values.hasNext()) {
      try {
        oos.writeObject(values.next());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void encode(Object value) {

  }
}
