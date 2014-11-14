package com.intel.picklepot.columnar;

import com.intel.picklepot.StopWatch;
import com.intel.picklepot.columnar.runlength.RunLengthIntegerWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class RunLengthEncoder implements Encoder {
  private OutputStream os;

  @Override
  public OutputStream getOutputStream() {
    if (os == null)
      os = new ByteArrayOutputStream();
    return os;
  }

  @Override
  public void encode(Iterator values, int num) {
    if (!values.hasNext()) {
      return;
    }
    StopWatch.start();
    Object obj = values.next();
    if (obj.getClass() != Integer.class) {
      System.err.println("unsupported type:" + obj.getClass());
      return;
    }
    os = new ByteArrayOutputStream();
    RunLengthIntegerWriter writer = new RunLengthIntegerWriter(os, true, 4, true);
    try {
      writer.write((Integer) obj);
      while (values.hasNext()) {
        writer.write(((Integer) values.next()));
      }
      writer.flush();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    StopWatch.stop("compress Integer");
  }

  @Override
  public void encode(Object value) {
  }
}
