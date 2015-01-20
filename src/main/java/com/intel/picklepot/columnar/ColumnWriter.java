package com.intel.picklepot.columnar;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.Block;
import com.intel.picklepot.storage.SimpleDataOutput;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.io.api.Binary;

import java.io.IOException;

public class ColumnWriter {
  private ValuesWriter valuesWriter;
  private SimpleDataOutput output;
  private int numValues = 0;
  private ColumnStatistics statistics;

  public ColumnWriter(ValuesWriter valuesWriter, SimpleDataOutput output) {
    this.output = output;
    this.valuesWriter = valuesWriter;
    this.statistics = new ColumnStatistics();
  }

  public void write(Object value) {
    numValues++;
    if (value.getClass().equals(String.class)) {
      valuesWriter.writeBytes(Binary.fromByteArray(((String) value).getBytes()));
    } else if(value.getClass().equals(Integer.class) || value.getClass().equals(int.class)){
      valuesWriter.writeInteger((Integer) value);
    } else {
      ((ObjectValuesWriter)valuesWriter).writeObject(value);
    }
    try {
      statistics.add(value);
    } catch (PicklePotException e) {
      e.printStackTrace();
    }
  }

  public void writeToBlock() {
    try {
      byte[] bytes = valuesWriter.getBytes().toByteArray();
      Block dictBlock = null;
      if(valuesWriter.getEncoding().usesDictionary()) {
        DictionaryPage dictPage = valuesWriter.createDictionaryPage();
        dictBlock = new Block(dictPage.getEncoding(), dictPage.getDictionarySize(), dictPage.getBytes().toByteArray());
      }
      Block dataBlock = new Block(valuesWriter.getEncoding(), numValues, bytes);
      output.writeBlock(dataBlock, dictBlock);
      statistics.print(dataBlock, dictBlock);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
