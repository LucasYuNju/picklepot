package com.intel.picklepot.column;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.serialization.Type;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;

import javax.xml.crypto.Data;
import java.io.IOException;

public abstract class ColumnReader {
  protected ValuesReader valuesReader;
  protected Block dataBlock;
  protected int numRead;

  public ColumnReader() {
  }

  public ColumnReader(DataInput input, Type type) {
    this.dataBlock = input.readBlock();
    Encoding encoding = dataBlock.getEncoding();
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, type.toParquetType(), 0, 0);
    this.valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
    try {
      valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public abstract Object read();

  public boolean hasNext() {
    return numRead < dataBlock.getNumValues();
  }

  protected Dictionary getDictioinary(Block dictBlock, Encoding encoding, ColumnDescriptor descriptor) {
    DictionaryPage dictPage = new DictionaryPage(BytesInput.from(dictBlock.getBytes()), dictBlock.getBytes().length, dictBlock.getNumValues(), dictBlock.getEncoding());
    try {
      return encoding.initDictionary(descriptor, dictPage);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
