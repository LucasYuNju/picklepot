package com.intel.picklepot;

import org.junit.Test;

import java.io.*;

//loaded 520 classes
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

  public void toFile() throws Exception {

    for(int i=0; i<100000; i++) {
      PicklePot picklepot = new PicklePotImpl(new FileInputStream("serialized"));
      synchronized (this) {
        while (picklepot.hasNext()) {
          Object restored = picklepot.read();
          System.out.println(restored);
          wait(10);
        }
      }
    }
  }

  public void testPiclePot() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Object> picklePot = new PicklePotImpl<Object>(baos, null);

    for(int i=0; i<100000; i++) {
      SimpleTest<Object> obj = new SimpleTest<Object>();
      obj.init(1, new Pair("a", 123));
      picklePot.write(obj);
    }

    picklePot.flush();
    picklePot.close();
    //1800B

    PicklePot picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    synchronized (this) {
      while (picklepot.hasNext()) {
        Object restored = picklepot.read();
        System.out.println(restored);
        wait(10);
      }
    }
  }

  public void repeat() throws Exception {
    synchronized (this) {
      for (int i = 0; i < 10000; i++) {
        testPiclePot();
        toFile();
        wait(10);
      }
    }
  }

  @Test
  public void main() throws Exception {
      new SimpleTest().testPiclePot();
  }
}
