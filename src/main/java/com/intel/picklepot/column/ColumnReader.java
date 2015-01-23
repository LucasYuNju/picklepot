package com.intel.picklepot.column;

import com.intel.picklepot.format.Block;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;

import java.io.IOException;

public abstract class ColumnReader {
  protected ValuesReader valuesReader;
  protected Block dataBlock;
  protected int numRead;

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
