package com.intel.picklepot.columnar;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.SimpleDataOutput;
import com.intel.picklepot.serialization.FieldType;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesWriter;
import parquet.io.api.Binary;

import java.io.IOException;

public class ColumnWriter {
  private ValuesWriter valuesWriter;
  private SimpleDataOutput output;
  private ColumnStatistics statistics;
  private FieldType type;
  private int numValues;

  public ColumnWriter(ValuesWriter valuesWriter, SimpleDataOutput output, Class clazz) {
    this.output = output;
    this.valuesWriter = valuesWriter;
    this.statistics = new ColumnStatistics();
    this.type = Utils.toFieldType(clazz);
  }

  public void write(Object value) throws PicklePotException {
    numValues++;
    if(Utils.toFieldType(value.getClass()) != type) {
      throw new PicklePotException("type mismatch! expected:" + type + ", actual:" + Utils.toFieldType(value.getClass()));
    }

    if (type == FieldType.STRING) {
      valuesWriter.writeBytes(Binary.fromByteArray(((String) value).getBytes()));
    } else if(type == FieldType.INT){
      valuesWriter.writeInteger((Integer) value);
    } else {
      ((ObjectValuesWriter)valuesWriter).writeObject(value);
    }
    statistics.add(value);
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
