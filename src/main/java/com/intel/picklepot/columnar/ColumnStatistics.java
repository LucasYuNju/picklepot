package com.intel.picklepot.columnar;

import com.intel.picklepot.Order;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.Block;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 * picklepot compress column values with parquet, then recompress them with snappy
 * As a comparison, we write plain column values, then compress them with snappy.
 */
public class ColumnStatistics {
  private static boolean enabled;
  private static int columnCount = -1;
  private ByteArrayOutputStream rawDataStream;

  public ColumnStatistics() {
    rawDataStream = new ByteArrayOutputStream();
    if(++columnCount == 0 && enabled) {
      System.out.printf("%-13s %15s %15s %15s\n","column name", "initial size", "snappy", "parquet+snappy");
    }
  }

  public void add(Object value) throws PicklePotException {
    if(!enabled)
      return;
    try {
      if (value.getClass().equals(String.class)) {
        rawDataStream.write(((String) value).getBytes());
      } else if(value.getClass().equals(Integer.class) || value.getClass().equals(int.class)){
        rawDataStream.write(ByteBuffer.allocate(4).putInt((Integer) value).array());
      } else {
        throw new PicklePotException("unable to write plain value of " + value.getClass());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void print(Block dataBlock, Block dictBlock) {
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
}
