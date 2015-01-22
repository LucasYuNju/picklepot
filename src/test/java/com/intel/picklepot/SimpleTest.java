package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class SimpleTest implements Serializable{

  public void testPiclePot() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    NewPicklePotImpl<String> picklePot = new NewPicklePotImpl<String>(baos, null);
    picklePot.add("aaa");
    picklePot.add("bbb");
    picklePot.flush();
    picklePot.close();

    NewPicklePotImpl picklepot = new NewPicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Object obj = picklepot.deserialize();
    System.out.println(obj);
    obj = picklepot.deserialize();
    System.out.println(obj);
  }

  public static void main(String args[]) throws IOException {
    try {
      new SimpleTest().testPiclePot();
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }
}
