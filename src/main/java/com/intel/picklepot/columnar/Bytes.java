package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Bytes {
  private static final byte split = '\01';

  public static byte[] toBytes(Iterator values) {
    if (!values.hasNext())
      return new byte[0];
    Object obj = values.next();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      if (obj.getClass() == String.class) {
        stringToByte((String) obj, bos);
        stringToByte(values, bos);
      } else if (obj.getClass() == Integer.class) {
        integerToByte((Integer) obj, bos);
        integerToByte(values, bos);
      } else {
        System.err.println("unsupported type:" + obj.getClass().getName());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return bos.toByteArray();
  }

  public static Iterator toPrimitiveType(byte[] bytes, String className) {
    className = className.replace("class", "").trim();
    if (String.class.getName().equals(className)) {
      return byteToString(bytes);
    } else if (Integer.class.getName().equals(className) || className.equals("int")) {
      return byteToInteger(bytes);
    } else {
      return null;
    }
  }

  private static void stringToByte(String str, ByteArrayOutputStream bos) throws IOException {
    bos.write(str.getBytes());
    bos.write(new byte[]{split});
  }

  private static void stringToByte(Iterator values, ByteArrayOutputStream bos) throws IOException {
    while (values.hasNext()) {
      String str = (String) values.next();
      stringToByte(str, bos);
    }
  }

  private static void integerToByte(Integer integer, ByteArrayOutputStream bos) throws IOException {
    bos.write(ByteBuffer.allocate(4).putInt(integer).array());
  }

  private static void integerToByte(Iterator values, ByteArrayOutputStream bos) throws IOException {
    while (values.hasNext()) {
      Integer integer = (Integer) values.next();
      integerToByte(integer, bos);
    }
  }

  private static Iterator byteToString(byte[] bytes) {
    int begin = 0;
    List<String> list = new LinkedList<String>();
    for (int i = 0; i < bytes.length; i++) {
      if (bytes[i] == split) {
        String str = null;
        try {
          str = new String(bytes, begin, i - begin, "utf-8");
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
        list.add(str);
        begin = i + 1;
      }
    }
    return list.iterator();
  }

  private static Iterator byteToInteger(byte[] bytes) {
    List<Integer> list = new LinkedList<Integer>();
    int sizeofInt = Integer.SIZE / Byte.SIZE;
    for (int i = 0; i < bytes.length / sizeofInt; i++) {
      final ByteBuffer bb = ByteBuffer.wrap(bytes, i * sizeofInt, sizeofInt);
      list.add(bb.getInt());
    }
    return list.iterator();
  }
}
