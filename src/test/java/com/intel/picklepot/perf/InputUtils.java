package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;
import com.intel.picklepot.column.Statistics;
import scala.Tuple2;
import scala.Tuple5;
import scala.Tuple8;

import java.io.*;
import java.util.*;

public class InputUtils {
  private static List<Object> objects;
  private static long dataSize;

  static List<Object> getObjects() throws IOException {
    getOrders();
//    getTuples();
    return objects;
  }

  private static List<Object> getPairs() throws IOException {
    File inFile = new File("pairs.data");
    if(objects != null)
      return objects;
    objects = new ArrayList<Object>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inFile)));
    String line;
    while((line = reader.readLine()) != null){
      String[] strs = line.split(" ");
      Pair p = new Pair(strs[0], Integer.parseInt(strs[1]));
      dataSize += strs[0].length() + 4;
      objects.add(p);
    }
    System.out.printf("dataSize:%,d\n", dataSize);
    return objects;
  }

  public static List<Object> getOrders() {
    if(objects == null) {
      objects = new ArrayList<Object>();
      try {
        BufferedReader reader = new BufferedReader(new FileReader(new File("data/orders.tbl")));
        String line;
        while((line = reader.readLine()) != null) {
          objects.add(new Statistics.Order(line));
        }
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return objects;
  }

  public static List<Object> getTuples() {
    if(objects == null) {
      List<Object> orders = getOrders();
      objects = new ArrayList<Object>();
      for(Object order : orders) {
        Statistics.Order o = (Statistics.Order) order;
        objects.add(new Tuple2<String, Tuple5>("a", new Tuple5<String, String, String, String, String>(o.clerk, o.comment, o.date, o.orderPriority, o.orderStatus)));
      }
    }
    return objects;
  }

  public static long getDataSize() {
    return dataSize;
  }
}