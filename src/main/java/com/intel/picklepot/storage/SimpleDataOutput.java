package com.intel.picklepot.storage;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Use ObjectOutputStream to write the data
 */
public class SimpleDataOutput implements DataOutput {
  ObjectOutputStream out;
  int numWrittenArray;
  int arrayLimit;

  public SimpleDataOutput(OutputStream outputStream) throws IOException {
    out = new ObjectOutputStream(outputStream);
  }

  /**
   * should be invoked before writeFieldByte(byte[] bytes)
   * @param classInfo
   * @throws IOException
   */
  public void writeClassInfo(ClassInfo classInfo) throws IOException {
      numWrittenArray = 0;
      arrayLimit = classInfo.getFieldInfos().size();
      out.writeObject(classInfo);
  }

  /**
   * write bytes to internal storage.
   * @param bytes
   */
  public void writeFieldByte(byte[] bytes) throws IOException, PicklePotException {
      if(++numWrittenArray > arrayLimit)
          throw  new PicklePotException("try to write more byte array than the number of class field");
      out.writeObject(bytes);
  }

  @Override
  public void writeBytes(byte[] bytes) {

  }

  @Override
  public void flush() {
    try {
      out.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
