package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public class SimpleTest<T> implements Serializable{
  long l;
  float f;
  double d;
  String s;
  Integer[] a;
  T t;

  public void init(int val, T t) {
    l = val;
    f = val + 0.1f;
    d = f;
    s = "a" + val;
    a = new Integer[]{val, val};
    this.t = t;
  }

  public static void testPiclePot() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    SimpleTest<Object> obj = new SimpleTest<Object>();
    obj.init(1, new Pair("a", 123));
    PicklePotImpl<Object> picklePot = new PicklePotImpl<Object>(baos, null);
    picklePot.write(obj);
    picklePot.write(obj);
    picklePot.flush();
    picklePot.close();

    PicklePotImpl picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Object restored = picklepot.read();
    System.out.println(restored);
    restored = picklepot.read();
    System.out.println(restored);
  }

  public static void testNonNested() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePotImpl<Object> picklePot = new PicklePotImpl<Object>(baos, null);
    picklePot.write(1.1);
    picklePot.write(1.1);
    picklePot.flush();
    picklePot.close();

    PicklePotImpl picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Object obj = picklepot.read();
    System.out.println(obj);
    obj = picklepot.read();
    System.out.println(obj);
  }

  public static void main(String args[]) throws Exception {
    try {
      testPiclePot();
      testNonNested();
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }
}
