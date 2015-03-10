package com.intel.picklepot;

import org.junit.Test;

import java.io.*;

//loaded 520 classes
public class SimpleTest<T> implements Serializable{
  long l;
  float f;
  double d;
  String s;
  Pair[] a;
  double[] da;
  T t;

  public void init(int val, T t) {
    l = val;
    f = val + 0.1f;
    d = f;
    s = "a" + val;
    a = new Pair[]{new Pair("a", 123), new Pair("b", 456)};
    da = new double[] {1.0, 2.0, 3.0};
    this.t = t;
  }

  public void testPiclePot() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Object> picklePot = new PicklePotImpl<Object>(baos, null);

    for(int i=0; i<10; i++) {
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
        wait(10);
      }
    }
  }

  @Test
  public void main() throws Exception {
      new SimpleTest().testPiclePot();
  }
}
