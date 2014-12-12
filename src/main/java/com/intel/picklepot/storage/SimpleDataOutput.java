package com.intel.picklepot.storage;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.Block;
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
  public void writeClassInfo(ClassInfo classInfo) {
      numWrittenArray = 0;
      arrayLimit = classInfo.getFieldInfos().size();
    try {
      out.writeObject(classInfo);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * write bytes to internal storage.
   * @param bytes
   */
  public void writeFieldByte(byte[] bytes) throws PicklePotException {
      if(++numWrittenArray > arrayLimit)
          throw new PicklePotException("try to write more byte array than the number of class field");
    try {
      out.writeObject(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void writeBytes(byte[] bytes) {
    try {
      out.writeObject(bytes);
    } catch (IOException e) {
      e.printStackTrace();
    }
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

  public void writeBlock(Block dataBlock, Block dictBlock) {
    try {
      out.writeObject(dataBlock);
      if(dictBlock != null)
        out.writeObject(dictBlock);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
