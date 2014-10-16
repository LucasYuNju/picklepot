package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class SimpleTest {
  @Test
  public void test() throws IOException, PicklePotException, ClassNotFoundException {
    PicklePotImpl<Pair> picklePot = new PicklePotImpl<Pair>();
    File file = new File("test.ser");

    //serialize
    picklePot.initialize(Pair.class, new SimpleDataOutput(new FileOutputStream(file)), null);
    Pair wc1 = new Pair("hello", 1);
    Pair wc2 = new Pair("world", 2);
    picklePot.add(wc1);
    picklePot.add(wc2);
    picklePot.flush();

    //deserialize
    SimpleDataInput dataInput = new SimpleDataInput();
    dataInput.initialize(new FileInputStream(file));
    Iterator iterator = picklePot.deserialize(dataInput);
    assertEquals(iterator.next(), wc1);
    assertEquals(iterator.next(), wc2);
    assertFalse(iterator.hasNext());
  }
}
