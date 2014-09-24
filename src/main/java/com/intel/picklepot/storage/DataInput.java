package com.intel.picklepot.storage;

public interface DataInput {

  public void readBytes(byte[] bytes);

  public void close();
}
