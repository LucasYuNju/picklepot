
package com.intel.picklepot.io;

public interface DataOutput {

  /**
   * write bytes to internal storage.
   * @param bytes
   */
  public void writeBytes(byte[] bytes);

  public void flush();

  public void close();
}