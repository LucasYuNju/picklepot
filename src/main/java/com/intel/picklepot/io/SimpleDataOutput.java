package com.intel.picklepot.io;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldGroup;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Use ObjectOutputStream to write the serialized objects
 */
public class SimpleDataOutput implements DataOutput {
  private ObjectOutputStream out;

  public SimpleDataOutput(OutputStream outputStream) {
    try {
      out = new ObjectOutputStream(outputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void writeFieldGroup(FieldGroup fieldGroup) {
    try {
      out.writeObject(fieldGroup);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void writeBlock(Block dataBlock) {
    try {
      out.writeObject(dataBlock);
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
}
