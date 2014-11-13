package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class Bytes {
  static final byte split = '\01';

  public static byte[] toBytes(Iterator<String> values) {
    if (!values.hasNext())
      return new byte[0];
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
        stringToByte(values, bos);
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

  private static void stringToByte(Iterator<String> values, ByteArrayOutputStream bos) throws IOException {
    while (values.hasNext()) {
      bos.write(values.next().getBytes(StandardCharsets.UTF_8));
      bos.write(split);
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
        String str = new String(bytes, begin, i - begin, StandardCharsets.UTF_8);
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
