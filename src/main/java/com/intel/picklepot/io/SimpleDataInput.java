package com.intel.picklepot.io;

import com.intel.picklepot.exception.PicklePotException;
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

  public SimpleDataInput(InputStream inputStream) throws PicklePotException {
    try {
      in = new ObjectInputStream(inputStream);
    } catch (IOException e) {
      throw new PicklePotException(e);
    }
  }

  @Override
  public Block readBlock() throws PicklePotException {
    try {
      return (Block)in.readObject();
    } catch (Exception e) {
      throw new PicklePotException(e);
    }
  }

  @Override
  public FieldGroup readFieldGroup() throws PicklePotException {
    try {
      if (in.available() == 0) {
        return null;
      }
      return (FieldGroup) in.readObject();
    } catch (IOException e) {
      throw new PicklePotException(e);
    } catch (ClassNotFoundException e) {
      throw new PicklePotException(e);
    }
  }

  @Override
  public void close() throws PicklePotException {
    try {
      in.close();
    } catch (IOException e) {
      throw new PicklePotException(e);
    }
  }
}
