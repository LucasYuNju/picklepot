package com.intel.picklepot;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class SimpleTest implements Serializable{
  public String innerStr = "hw";

  public static void main(String args[]) throws IOException {
    Object s = new SimpleTest();
    ObjectOutputStream oos = new ObjectOutputStream(new ByteArrayOutputStream());
    oos.writeObject(s);
    oos.close();
  }
}
