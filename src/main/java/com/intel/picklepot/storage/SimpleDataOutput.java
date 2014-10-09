package com.intel.picklepot.storage;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.ClassInfo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Use ObjectOutputStream to write the data
 */
public class SimpleDataOutput implements DataOutput {
  ObjectOutputStream out;

  public SimpleDataOutput(OutputStream outputStream) throws IOException {
    out = new ObjectOutputStream(outputStream);
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

  public void writeObjects(ClassInfo classInfo, List<byte[]> fieldsByte) throws PicklePotException, IOException {
    if (fieldsByte.size() != classInfo.getFieldInfos().size()) {
      throw new PicklePotException("Number of fields mismatch.");
    }
    out.writeObject(classInfo);
    for (byte[] fieldByte : fieldsByte) {
      out.writeObject(fieldByte);
    }
    close();
  }
}
