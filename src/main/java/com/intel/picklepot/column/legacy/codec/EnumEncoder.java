package com.intel.picklepot.column.legacy.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EnumEncoder implements Encoder<String> {
  private ByteArrayOutputStream out;

  @Override
  public OutputStream getOutputStream() {
    return out;
  }

  /**
   * write dictionary size, dictionary, number of strings, and indices of strings
   * in the dictionary repectively
   * @param values Strings to encode
   */
  @Override
  public void encode(Iterator<String> values, int num) {
    out = new ByteArrayOutputStream();
    Map<String, Integer> dict = new LinkedHashMap<String, Integer>();
    List<String> strs = new ArrayList<String>();
    while(values.hasNext()) {
      String str = values.next();
      strs.add(str);
      if(!dict.containsKey(str)) {
        dict.put(str, dict.size());
      }
    }
    out.write(dict.size());
    try {
      for(String word : dict.keySet()) {
        out.write(word.getBytes(StandardCharsets.UTF_8));
        out.write(Bytes.split);
      }
      out.write(ByteBuffer.allocate(4).putInt(strs.size()).array());
      for(String str : strs) {
        int integer = dict.get(str);
        out.write(dict.get(str));
      }
      out.flush();
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void encode(String value) {

  }

  /**
   * check whether Strings should be encoded with EnumEncoder
   * @param values Strings to check
   * @return true if EnumEncoder is applicable to values
   */
  public static boolean applicable(Iterator<String> values) {
    Map<String, Integer> dict = new LinkedHashMap<String, Integer>();
    int numStr = 0;
    while(values.hasNext()) {
      numStr++;
      String str = values.next();
      if(!dict.containsKey(str)) {
        dict.put(str, dict.size());
      }
    }
    if(dict.size()>Byte.MAX_VALUE || dict.size()>numStr/2)
      return false;
    return true;
  }
}
