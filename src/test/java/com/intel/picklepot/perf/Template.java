package com.intel.picklepot.perf;

import java.util.Iterator;

public abstract class Template {
  byte[] serialized;
  byte[] compressed;
  Iterator restored;
  String name;
  int repeations;
  long serialTimeNanos;
  long deserialTimeNanos;

  public Template(String name, int repeations) {
    this.name = name;
    this.repeations = repeations;
  }

  public void test() {
    try {
      serialTimeNanos = 0;
      for (int i = 0; i < repeations; i++) {
        long startTime = System.nanoTime();
        serialize();
        serialTimeNanos += System.nanoTime() - startTime;
        System.gc();
      }
      serialTimeNanos /= repeations;

      deserialTimeNanos = 0;
      for (int i = 0; i < repeations; i++) {
        long startTime = System.nanoTime();
        deserialize();
        deserialTimeNanos += System.nanoTime() - startTime;
        System.gc();
      }
      deserialTimeNanos /= repeations;

      if (!verifyDeserialized()) {
        System.err.println("deserialization fault");
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void printStatistics() {
    System.out.printf("%-20s", name);
//    System.out.printf("serialSize:%,d serialTime:%,dms serialSpeed:%,dMB/s deserialTime:%,dms, deserialSpeed:%,dMB\n",
//        getSerialiedSize(), serialTimeNanos / 1000000,
//            InputUtils.getDataSize() * 1000 / serialTimeNanos,
//            deserialTimeNanos / 1000000,
//            InputUtils.getDataSize() * 1000 / deserialTimeNanos);
    System.out.printf("size:%,d code time:%,dms decode time:%,dms\n", compressed.length, serialTimeNanos/1000000, deserialTimeNanos/1000000);
    serialized = null;
    compressed = null;
    restored = null;
  }

  private long getSerialiedSize() {
    if(serialized == null)
      return 0;
    return serialized.length;
  }

  protected abstract void serialize() throws Exception;

  protected abstract void deserialize() throws Exception;

  protected abstract boolean verifyDeserialized() throws Exception;

}
