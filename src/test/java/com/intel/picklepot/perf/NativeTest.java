package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class NativeTest extends Template {

  public NativeTest(int repeations) {
    super("java native", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    List<Pair> pairs = InputUtils.getPairs();
    ObjectOutputStream objOutputStream = new ObjectOutputStream(outputStream);
    objOutputStream.writeLong(pairs.size());
    for(Pair p : pairs) {
      objOutputStream.writeObject(p);
    }
    objOutputStream.close();
    serialized = outputStream.toByteArray();
  }

  @Override
  protected void deserialize() throws Exception {
    List<Pair> pairs = new LinkedList<Pair>();
    ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(serialized));
    long num = inputStream.readLong();
    for(int i=0; i<num; i++) {
      pairs.add((Pair) inputStream.readObject());
    }
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    return true;
  }
}
