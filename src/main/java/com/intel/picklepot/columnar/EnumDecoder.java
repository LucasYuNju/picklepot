package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
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

//  public static void main(String args[]) {
//    List<String> input = new ArrayList<String>();
//    String[] dict = new String[]{"huffmana", "smappyaa", "runlengt", "deflatea"};
//    Random random = new Random();
//    for(int i=0; i<10000; i++) {
//      input.add(dict[random.nextInt(dict.length)]);
//    }
//    EnumEncoder encoder = new EnumEncoder();
//    EnumDecoder decoder = new EnumDecoder();
//    encoder.encode(input.iterator());
//    byte[] encoded = ((ByteArrayOutputStream)encoder.getOutputStream()).toByteArray();
//    Iterator<String> decoded = decoder.decode(encoded, String.class.getName());
//    return;
//  }
}
