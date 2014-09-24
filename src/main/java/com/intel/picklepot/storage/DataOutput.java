package com.intel.picklepot.storage;

public interface DataOutput {

  public void writeBytes(byte[] bytes);

  public void close();
}
