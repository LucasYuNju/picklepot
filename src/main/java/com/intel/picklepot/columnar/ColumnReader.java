package com.intel.picklepot.columnar;

import com.intel.picklepot.metadata.Block;
import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;
import parquet.schema.PrimitiveType;

import java.io.IOException;

public class ColumnReader {
  private ValuesReader valuesReader;
  private Block dataBlock;
  private Class columnClass;
  private int numRead;

  //parquet.column.Encoding will find compatible ValuesReader
  public ColumnReader(Block dataBlock, Block dictBlock, Class columnClass) throws IOException {
    this.dataBlock = dataBlock;
    this.columnClass = columnClass;
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, getPrimitiveTypeName(columnClass), 0, 0);
    Encoding encoding = dataBlock.getEncoding();
    if(dataBlock.getEncoding().usesDictionary()) {
      DictionaryPage dictPage = new DictionaryPage(BytesInput.from(dictBlock.getBytes()), dictBlock.getBytes().length, dictBlock.getNumValues(), dictBlock.getEncoding());
      Dictionary dict = encoding.initDictionary(descriptor, dictPage);
      this.valuesReader = dataBlock.getEncoding().getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dict);
    }
    else {
      this.valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
    }
    this.valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
  }

  public Object read() {
    if(!hasNext())
      return null;
    numRead++;
    if(columnClass == String.class)
      return new String(valuesReader.readBytes().getBytes());
    return valuesReader.readInteger();
  }

  public boolean hasNext() {
    return numRead < dataBlock.getNumValues();
  }

  private PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(Class clazz) {
    if(clazz==String.class)
      return PrimitiveType.PrimitiveTypeName.BINARY;
    return PrimitiveType.PrimitiveTypeName.INT32;
  }
}
