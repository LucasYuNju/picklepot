package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import com.intel.picklepot.StopWatch;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Encoder<T> implements Encoder<T> {
  private OutputStream os;

  @Override
  public OutputStream getOutputStream() {
    if (os == null)
      os = new ByteArrayOutputStream();
    return os;
  }

  @Override
  public void encode(Iterator<T> values) {
    StopWatch.start();
    byte[] input = Bytes.toBytes((Iterator<String>)values);
    StopWatch.stop("Iterator<String> to byte["+input.length+"]");

    StopWatch.start();
    LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
    byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
    int compressedSize = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);
    os = new ByteArrayOutputStream();
    try {
      os.write(compressed, 0, compressedSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
    StopWatch.stop("compress String");
  }

  @Override
  public void encode(Object value) {

  }
}
