package com.intel.picklepot.column;

import com.intel.picklepot.format.Block;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * picklepot compress column values with parquet, then recompress them with snappy
 * As a comparison, we write plain column values, then compress them with snappy.
 */
public class Statistics {
  private static boolean enabled;
  private static int columnCount = -1;
  private ByteArrayOutputStream rawDataStream;

  public Statistics() {
    rawDataStream = new ByteArrayOutputStream();
    if(++columnCount == 0 && enabled) {
      System.out.printf("%-13s %15s %15s %15s\n","column name", "initial size", "snappy", "parquet+snappy");
    }
  }

  public void add(Object value) {
    if(!enabled)
      return;
    try {
      if (value.getClass().equals(String.class)) {
        rawDataStream.write(((String) value).getBytes());
      } else if(value.getClass().equals(Integer.class) || value.getClass().equals(int.class)){
        rawDataStream.write(ByteBuffer.allocate(4).putInt((Integer) value).array());
      } else {
        throw new IllegalArgumentException("unable to write plain value of " + value.getClass());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void print(Block dataBlock, Block dictBlock) {
    if(!enabled)
      return;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(dataBlock);
      oos.writeObject(dictBlock);
      byte[] rawData = rawDataStream.toByteArray();
      byte[] snappyed = Snappy.compress(rawData);
      byte[] serialized = baos.toByteArray();
      byte[] serialized_snappyed = Snappy.compress(serialized);
      String columnName = Order.class.getDeclaredFields()[columnCount % 7].getName();
      System.out.printf("%-13s %,15d %,15d %,15d\n", columnName, rawData.length, snappyed.length, serialized_snappyed.length);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void enable() {
    enabled = true;
  }

  public static void disable() {
    enabled = false;
  }


  public static class Order implements Serializable {
    public int orderKey;
    public int custKey;
    public String orderStatus;
    //skip price column
    public String date;
    public String orderPriority;
    public String clerk;
    public int shipProprity;
    //skip comment column
    public String comment;

    public Order(String line) {
      String[] values = line.split("\\|");
      orderKey = Integer.parseInt(values[0]);
      custKey = Integer.parseInt(values[1]);
      orderStatus = values[2];
      date = values[4];
      orderPriority = values[5];
      clerk = values[6];
      shipProprity = Integer.parseInt(values[7]);
      comment = values[8];
    }

    public Order() {}

    @Override
    public boolean equals(Object object) {
      if (object instanceof Order) {
        Order o = (Order) object;
        return orderKey == o.orderKey
            && custKey == o.custKey
            && orderStatus.equals(o.orderStatus)
            && date.equals(o.date)
            && orderPriority.equals(o.orderPriority)
            && clerk.equals(o.clerk)
            && shipProprity == o.shipProprity;
      }
      return false;
    }
  }

  public static class StopWatch {
    private static long startTimeNanos;
    public static boolean enabled = false;

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }

    public static void start() {
      startTimeNanos = System.nanoTime();
    }

    /**
     * @return nano time since last start
     */
    public static void stop(String info) {
      long time = System.nanoTime() - startTimeNanos;
      if(enabled)
        System.out.printf(info + ":%,dms\n", time / 1000000);
    }
  }
}


