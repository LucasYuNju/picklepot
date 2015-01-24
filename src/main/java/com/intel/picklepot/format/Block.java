package com.intel.picklepot.format;

import parquet.column.Encoding;

import java.io.Serializable;

/**
 * if data block's encoding is PLAIN_DICTIONARY, a dictionay block follows
 */
public class Block implements Serializable{
  private Encoding encoding;
  private int numValues;
  private byte[] bytes;

  public Block(Encoding encoding, int numValues, byte[] bytes) {
    this.encoding = encoding;
    this.numValues = numValues;
    this.bytes = bytes;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public int getNumValues() {
    return numValues;
  }

  public byte[] getBytes() {
    return bytes;
  }
}
