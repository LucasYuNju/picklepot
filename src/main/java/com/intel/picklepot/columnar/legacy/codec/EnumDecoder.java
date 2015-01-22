package com.intel.picklepot.columnar.legacy.codec;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

public class EnumDecoder implements Decoder<String> {

  @Override
  public Iterator<String> decode(byte[] bytes, String className) {
    List<String> dict = new ArrayList<String>();
    List<String> strs = new ArrayList<String>();
    int index = 0, lastSplit = 0;
    int dictSize = bytes[index++];
    for ( ; dict.size()<dictSize; index++) {
      if (bytes[index] == Bytes.split) {
        String str = null;
        try {
          str = new String(bytes, lastSplit + 1, index - lastSplit - 1, "utf-8");
        } catch (UnsupportedEncodingException e) {
          e.printStackTrace();
        }
        dict.add(str);
        lastSplit = index;
      }
    }
    int numStr = ByteBuffer.wrap(bytes, index, 4).getInt();
    index+=4;
    for(int i=0; i<numStr; i++) {
      strs.add(dict.get(bytes[index++]));
    }
    return strs.iterator();
  }
}
