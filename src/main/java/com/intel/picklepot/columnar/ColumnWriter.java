package com.intel.picklepot.columnar;

import com.intel.picklepot.Order;
import com.intel.picklepot.metadata.Block;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.xerial.snappy.Snappy;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.io.api.Binary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ColumnWriter {
  private ValuesWriter valuesWriter;
  private SimpleDataOutput output;
  private int numValues = 0;
  private static boolean enableStatstics = false;
  private static List<ValuesWriter> writers = new ArrayList<ValuesWriter>();
  private static int columnCount = -1;
  private ByteArrayOutputStream rawDataOutput;

  public ColumnWriter(Class clazz, SimpleDataOutput output) {
    this.output = output;
    this.valuesWriter = writers.get(++columnCount % 7);
    rawDataOutput = new ByteArrayOutputStream();
  }

  public void write(Object value) {
    ++numValues;
    try {
      if (value.getClass().equals(String.class)) {
        valuesWriter.writeBytes(Binary.fromByteArray(((String) value).getBytes()));
        if(enableStatstics)
          rawDataOutput.write(((String) value).getBytes());
      } else {
        valuesWriter.writeInteger((Integer) value);
        if(enableStatstics)
          rawDataOutput.write(ByteBuffer.allocate(4).putInt((Integer) value).array());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void flush() {
    try {
      byte[] bytes = valuesWriter.getBytes().toByteArray();
      Block dictBlock = null;
      Block dataBlock = new Block(valuesWriter.getEncoding(), numValues, bytes);
      if(valuesWriter.getEncoding().usesDictionary()) {
        DictionaryPage dictPage = valuesWriter.createDictionaryPage();
        dictBlock = new Block(dictPage.getEncoding(), dictPage.getDictionarySize(), dictPage.getBytes().toByteArray());
      }
      output.writeBlock(dataBlock, dictBlock);

      if(enableStatstics) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(dataBlock);
        oos.writeObject(dictBlock);
        byte[] rawData = rawDataOutput.toByteArray();
        byte[] snappyed = Snappy.compress(rawData);
        byte[] serialized = baos.toByteArray();
        byte[] serialized_snappyed = Snappy.compress(serialized);
        String columnName = Order.class.getDeclaredFields()[columnCount % 7].getName();
        System.out.printf("%-13s %,15d %,15d %,15d\n", columnName, rawData.length, snappyed.length, serialized_snappyed.length);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void enableColumnStatics(boolean enable) {
    enableStatstics = enable;
    if(enableStatstics)
      System.out.printf("%-13s %15s %15s %15s\n","column name", "initial size", "snappy", "parquet+snappy");
  }

  public static void resetWriters() {
    int dictionaryPageSizeThreshold = 256 * 1024;
    int initialSizePerCol = 512 * 1024;
    writers.clear();
    //order key
    writers.add(new DeltaBinaryPackingValuesWriter(initialSizePerCol));
    //cust key
    writers.add(new DeltaBinaryPackingValuesWriter(initialSizePerCol));
    //order status
    writers.add(new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol));
    //perhaps not suitable for date column,
    writers.add(new DeltaByteArrayWriter(initialSizePerCol));
    //order priority
    writers.add(new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictionaryPageSizeThreshold, initialSizePerCol));
    //clerk
    writers.add(new DeltaByteArrayWriter(initialSizePerCol));
    //ship proirity
    writers.add(new DeltaBinaryPackingValuesWriter(initialSizePerCol));
  }
}
