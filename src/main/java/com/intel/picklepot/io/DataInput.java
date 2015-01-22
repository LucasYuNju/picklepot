package com.intel.picklepot.io;

import java.io.InputStream;

public interface DataInput {

  /**
   * initialize serialized source data, it must be invoked before read.
   * @param inputStream
   */
  public void initialize(InputStream inputStream);

  /**
   * read bytes from internal serialized source data.
   * @param bytes
   */
  public void read(byte[] bytes);

  /**
   * close DataInput, release all resources.
   */
  public void close();
}
