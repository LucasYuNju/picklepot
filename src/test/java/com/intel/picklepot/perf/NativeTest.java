package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class NativeTest extends Template {

  public NativeTest(int repeations) {
    super("java native+snappy", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    List<Object> pairs = InputUtils.getObjects();
    ObjectOutputStream objOutputStream = new ObjectOutputStream(outputStream);
    objOutputStream.writeLong(pairs.size());
    for(Object p : pairs) {
      objOutputStream.writeObject(p);
    }
    objOutputStream.close();
    serialized = outputStream.toByteArray();
    compressed = Snappy.compress(serialized);
    objOutputStream = null;
  }

  @Override
  protected void deserialize() throws Exception {
    List<Object> pairs = new LinkedList<Object>();
    ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(serialized));
    long num = inputStream.readLong();
    for(int i=0; i<num; i++) {
      pairs.add(inputStream.readObject());
    }
    inputStream = null;
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    return true;
  }
}
