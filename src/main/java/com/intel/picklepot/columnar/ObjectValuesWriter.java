package com.intel.picklepot.columnar;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * serialize column of unsupported type, such as [B, float
 */
public class ObjectValuesWriter extends ValuesWriter{
  private ByteArrayOutputStream baos;
  private ObjectOutputStream oos;

  public ObjectValuesWriter() {
    baos = new ByteArrayOutputStream();
    try {
      oos = new ObjectOutputStream(baos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public long getBufferedSize() {
    return 0;
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.from(baos.toByteArray());
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.BIT_PACKED;
  }

  @Override
  public void reset() {
    baos = new ByteArrayOutputStream();
    try {
      oos = new ObjectOutputStream(baos);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public long getAllocatedSize() {
    return 0;
  }

  @Override
  public String memUsageString(String s) {
    return null;
  }

  public void writeObject(Object obj) {
    try {
      oos.writeObject(obj);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
