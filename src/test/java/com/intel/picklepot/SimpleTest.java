package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import com.intel.picklepot.unsafe.NewPicklePotImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class SimpleTest implements Serializable{

  public void testPiclePot() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    NewPicklePotImpl<String> picklePot = new NewPicklePotImpl<String>();
    SimpleDataOutput sdo = new SimpleDataOutput(baos);
    picklePot.initialize(null, sdo, null);
    picklePot.add("");
    picklePot.add("");
    picklePot.flush();
    picklePot.close();

    NewPicklePotImpl picklepot = new NewPicklePotImpl();
    SimpleDataInput sdi = new SimpleDataInput();
    sdi.initialize(new ByteArrayInputStream(baos.toByteArray()));
    Object obj = picklepot.deserialize(sdi);
    return;
  }

  public static void main(String args[]) throws IOException {
    try {
      new SimpleTest().testPiclePot();
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }
}
