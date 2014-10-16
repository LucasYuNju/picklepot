package com.intel.picklepot.columnar;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.util.Iterator;

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
    LZ4Factory factory = LZ4Factory.fastestInstance();
    LZ4SafeDecompressor decompressor = factory.safeDecompressor();
    byte[] decompressed = new byte[bytes.length * maxCompressionRatio];
    int decompressedSize = decompressor.decompress(bytes, 0, bytes.length, decompressed, 0);
    while (decompressedSize > decompressed.length) {
      decompressed = new byte[decompressed.length * 2];
      decompressedSize = decompressor.decompress(bytes, 0, bytes.length, decompressed, 0);
    }
    byte[] truncated = new byte[decompressedSize];
    System.arraycopy(decompressed, 0, truncated, 0, decompressedSize);
    return Bytes.toPrimitiveType(truncated, className);
  }
}
