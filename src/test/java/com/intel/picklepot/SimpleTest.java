package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class SimpleTest implements Serializable{
  Pair p;

  public SimpleTest(String a, int b) {
    p = new Pair(a, b);
  }

  public static void testPiclePot() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePotImpl<Object> picklePot = new PicklePotImpl<Object>(baos, null);
    picklePot.write(new SimpleTest("a", 1));
    picklePot.write(new SimpleTest("bbb", 2));
    picklePot.flush();
    picklePot.close();

    PicklePotImpl picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Object obj = picklepot.read();
    System.out.println(obj);
    obj = picklepot.read();
    System.out.println(obj);
  }

  public static void main(String args[]) throws IOException {
    try {
      testPiclePot();
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }
}
