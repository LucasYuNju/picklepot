package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class SimpleTest implements Serializable{

  public void testPiclePot() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePotImpl<String> picklePot = new PicklePotImpl<String>(baos, null);
    picklePot.write("aa");
    picklePot.write("bbb");
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
      new SimpleTest().testPiclePot();
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }
}
