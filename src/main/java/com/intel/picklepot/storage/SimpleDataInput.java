package com.intel.picklepot.storage;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.format.Block;
import com.intel.picklepot.metadata.ClassInfo;
import com.intel.picklepot.unsafe.FieldGroup;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Use ObjectInputStream to read the data
 */
public class SimpleDataInput implements DataInput {
  private ObjectInputStream in;
  private ClassInfo classInfo;
  private List<byte[]> fieldsByte;

  @Override
  public void initialize(InputStream inputStream) {
    try {
      in = new ObjectInputStream(inputStream);
      fieldsByte = new ArrayList<byte[]>();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void read(byte[] bytes) {

  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void readObjects() throws PicklePotException {
    if (in == null || fieldsByte == null) {
      throw new PicklePotException("DataInput not initialized.");
    }
    if (classInfo == null) {
      try {
        classInfo = (ClassInfo) in.readObject();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public ClassInfo getClassInfo() {
    return classInfo;
  }

  public List<byte[]> getFieldsByte() {
    return fieldsByte;
  }

  public Block readBlock() {
    Block block = null;
    try {
      block = (Block)in.readObject();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return block;
  }

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
}
