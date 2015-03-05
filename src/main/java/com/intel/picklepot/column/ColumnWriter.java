package com.intel.picklepot.column;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.DataOutput;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;

import java.io.IOException;

public abstract class ColumnWriter {
  protected ValuesWriter valuesWriter;
  protected DataOutput output;
  protected Statistics statistics;
  protected int numValues;

  public ColumnWriter(DataOutput output) {
    this.output = output;
    this.statistics = new Statistics();
  }

  public abstract void write(Object value) throws PicklePotException;

  public void writeToBlock() {
    try {
      //ValuesWriter.getBytes() need to create a new Array and copy data.
      //To improve performance, see also parquet.bytes.CapacityByteArrayOutputStream.
      byte[] dataBytes = valuesWriter.getBytes().toByteArray();
      Block dataBlock = new Block(valuesWriter.getEncoding(), numValues, dataBytes);
      output.writeBlock(dataBlock);

      Block dictBlock = null;
      if(valuesWriter.getEncoding().usesDictionary()) {
        DictionaryPage dictPage = valuesWriter.createDictionaryPage();
        dictBlock = new Block(dictPage.getEncoding(), dictPage.getDictionarySize(), dictPage.getBytes().toByteArray());
        output.writeBlock(dictBlock);
      }
      statistics.print(dataBlock, dictBlock);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public String toString() {
    return valuesWriter.getEncoding().toString();
  }
}
