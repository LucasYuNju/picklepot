package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.SimpleDataOutput;

import java.io.*;

public class SimpleTest {
  public void function(OutputStream os) throws PicklePotException {
    PicklePotImpl pp = new PicklePotImpl();
    pp.initialize(String.class, new SimpleDataOutput(os), null);
    pp.add("abc");
    pp.flush();
    pp.close();
  }

  public static void main(String args[]) {
    File f = new File("/tmp/simpleTest");
//    f.createNewFile();

    OutputStream baos = null;
    try {
      baos = new FileOutputStream(f);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    SimpleTest st = new SimpleTest();

    try {
      st.function(baos);
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
    System.out.println(f.length());
    try {
      st.function(baos);
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
    System.out.println(f.length());
  }
}
