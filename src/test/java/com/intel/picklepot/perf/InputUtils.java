package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.Map.Entry;

public class InputUtils {
  //  private static File inFile = new File("enwik8");
  private static File inFile = new File("enwik6");
  private static List<Pair> pairs;
  private static long dataSize;

  static List<Pair> getPairs() throws FileNotFoundException {
    if(pairs != null)
      return pairs;

    Scanner in = new Scanner(inFile);
    Map<String, Integer> map = new HashMap<String, Integer>();
    while (in.hasNext()) {
      for (String str : in.nextLine().split("[\\s\\.<>#$%&*@,;\\]\\?\\[!_\"'“”\\(\\)\\+-/]")) {
        str = str.toLowerCase();
        if (!map.containsKey(str)) {
          map.put(str, 1);
        } else {
          map.put(str, map.get(str) + 1);
        }
      }
    }
    in.close();

    pairs = new LinkedList<Pair>();
    for (Entry<String, Integer> entry : map.entrySet()) {
      Pair p = new Pair(entry.getKey(), entry.getValue());
      pairs.add(p);
      dataSize += p.word.length() + 4;
    }
    System.out.printf("dataSize:%,d\n", dataSize);
    return pairs;
  }

  public static long getDataSize() {
    return dataSize;
  }
}