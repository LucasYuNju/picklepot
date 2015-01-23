package com.intel.picklepot.io;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldGroup;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 * Use ObjectInputStream as the internal stream to read serialized objects
 */
public class SimpleDataInput implements DataInput {
  private ObjectInputStream in;

  public SimpleDataInput(InputStream inputStream) {
    try {
      in = new ObjectInputStream(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Block readBlock() {
    try {
      return (Block)in.readObject();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public FieldGroup readFieldGroup() {
    FieldGroup ret = null;
    try {
      ret = (FieldGroup) in.readObject();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
