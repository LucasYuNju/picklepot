package com.intel.picklepot.storage;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;

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

  public void readObjects() throws PicklePotException, IOException, ClassNotFoundException {
    if (in == null || fieldsByte == null) {
      throw new PicklePotException("DataInput not initialized.");
    }
    if (classInfo == null) {
      classInfo = (ClassInfo) in.readObject();
    }
    int numField = classInfo.getFieldInfos().size();
    for (int i = 0; i < numField; i++) {
      try {
        byte[] fieldByte = (byte[]) in.readObject();
        fieldsByte.add(fieldByte);
      } catch (Exception e) {
        throw new PicklePotException("Error reading fields.", e);
      }
    }
    close();
  }

  public ClassInfo getClassInfo() {
    return classInfo;
  }

  public List<byte[]> getFieldsByte() {
    return fieldsByte;
  }
}
