package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.intel.picklepot.StopWatch;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Encoder<T> implements Encoder<T> {
  private OutputStream os;
  static int counter = 0;

  @Override
  public OutputStream getOutputStream() {
    if (os == null)
      os = new ByteArrayOutputStream();
    return os;
  }

  @Override
  public void encode(Iterator<T> values, int num) {
    int originalSize = 0;
    StopWatch.start();
    os = new ByteArrayOutputStream();
    Iterator<String> strs = (Iterator<String>) values;
    try {
      os.write(ByteBuffer.allocate(4).putInt(num).array());
      LZ4BlockOutputStream lz4 = new LZ4BlockOutputStream(os);
      while(strs.hasNext()) {
        String str = strs.next();
        lz4.write(str.getBytes());
        originalSize += str.length();
        lz4.write(new byte[]{Bytes.split});
      }
      lz4.flush();
      lz4.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if(++counter == 4) {
      System.out.println("original:" + originalSize);
      System.out.println("compressed:" + ((ByteArrayOutputStream)os).toByteArray().length);
    }
    StopWatch.stop("compress string");  }

  @Override
  public void encode(Object value) {

  }
}
