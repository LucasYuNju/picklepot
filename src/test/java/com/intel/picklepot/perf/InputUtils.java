package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class InputUtils {
  private static File inFile = new File("pairs.data");
  private static List<Pair> pairs;
  private static long dataSize;

  static List<Pair> getPairs() throws IOException {
    if(pairs != null)
      return pairs;
    pairs = new ArrayList<Pair>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inFile)));
    String line;
    while((line = reader.readLine()) != null){
      String[] strs = line.split(" ");
      Pair p = new Pair(strs[0], Integer.parseInt(strs[1]));
      dataSize += strs[0].length() + 4;
      pairs.add(p);
    }
    System.out.printf("dataSize:%,d\n", dataSize);
    return pairs;
  }

  public static long getDataSize() {
    return dataSize;
  }

//  static List<Pair> getPairs() throws FileNotFoundException {
//    if(pairs != null)
//      return pairs;
//    Scanner in = new Scanner(inFile);
//    Map<String, Integer> map = new HashMap<String, Integer>();
//    while (in.hasNext()) {
//      for (String str : in.nextLine().split("[^a-zA-Z]")) {
//        str = str.toLowerCase();
//        if(str.equals(""))
//          continue;
//        if (!map.containsKey(str)) {
//          map.put(str, 1);
//        } else {
//          map.put(str, map.get(str) + 1);
//        }
//      }
//    }
//    in.close();
//
//    pairs = new LinkedList<Pair>();
//    for (Entry<String, Integer> entry : map.entrySet()) {
//      Pair p = new Pair(entry.getKey(), entry.getValue());
//      pairs.add(p);
//      dataSize += p.word.length() + 4;
//    }
//    System.out.printf("dataSize:%,d\n", dataSize);
//    return pairs;
//  }

//  public static void main(String args[]) throws IOException {
//    List<Pair> pairs = getPairs();
//    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("pairs.data"))));
//    for(Pair p : pairs)
//      writer.write(p.word + " " + p.count + "\n");
//    writer.flush();
//    writer.close();
//  }
}