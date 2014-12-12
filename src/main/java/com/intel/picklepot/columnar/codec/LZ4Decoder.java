package com.intel.picklepot.columnar.codec;

import com.intel.picklepot.columnar.codec.Bytes;
import com.intel.picklepot.columnar.codec.Decoder;
import net.jpountz.lz4.LZ4BlockInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LZ4Decoder implements Decoder {
  private static final int maxCompressionRatio = 5;

  /**
   * performance could be improved
   *
   * @param bytes     compressed bytes
   * @param className class name of target object
   * @return iterator of object collection
   */
  @Override
  public Iterator decode(byte[] bytes, String className) {
    int numStr = ByteBuffer.wrap(bytes, 0, 4).getInt();
    List<String> res = new ArrayList<String>(numStr);
    LZ4BlockInputStream lz4 = new LZ4BlockInputStream(new ByteArrayInputStream(bytes, 4, bytes.length - 4));
    byte[] decompressed = new byte[64 * 1024];
    int toRead = 0;
    while (res.size() < numStr) {
      try {
        int numFetched = lz4.read(decompressed, toRead, decompressed.length - toRead);
        toRead += numFetched;
        int lastSplit = -1;
        for (int i = 0; i < toRead; i++) {
          if (decompressed[i] == Bytes.split) {
            String str = null;
            str = new String(decompressed, lastSplit + 1, i - lastSplit - 1, "utf-8");
            res.add(str);
            lastSplit = i;
          }
        }
        int remain = toRead - lastSplit - 1;
        System.arraycopy(decompressed, lastSplit + 1, decompressed, 0, remain);
        toRead = remain;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return res.iterator();
  }
}
