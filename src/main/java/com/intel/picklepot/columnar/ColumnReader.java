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
import java.io.ObjectInputStream;

public class ColumnReader {
  private ValuesReader valuesReader;
  private Block dataBlock;
  private Class columnClass;
  private int numRead;
  private ObjectInputStream ois;

  //parquet.column.Encoding will find compatible ValuesReader
  public ColumnReader(Block dataBlock, Block dictBlock, Class columnClass) throws IOException {
    this.dataBlock = dataBlock;
    this.columnClass = columnClass;
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, getPrimitiveTypeName(columnClass), 0, 0);
    Encoding encoding = dataBlock.getEncoding();
    if(dataBlock.getEncoding() == Encoding.BIT_PACKED) {
      valuesReader = new ObjectValuesReader();
    }
    else if(dataBlock.getEncoding().usesDictionary()) {
      DictionaryPage dictPage = new DictionaryPage(BytesInput.from(dictBlock.getBytes()),
          dictBlock.getBytes().length, dictBlock.getNumValues(), dictBlock.getEncoding());
      Dictionary dict = encoding.initDictionary(descriptor, dictPage);
      this.valuesReader = encoding.getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dict);
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
    else if(columnClass == Integer.class || columnClass == int.class) {
      return valuesReader.readInteger();
    }
    return ((ObjectValuesReader)valuesReader).readObject();
  }

  public boolean hasNext() {
    return numRead < dataBlock.getNumValues();
  }

  private PrimitiveType.PrimitiveTypeName getPrimitiveTypeName(Class clazz) {
    if(clazz==String.class)
      return PrimitiveType.PrimitiveTypeName.BINARY;
    return PrimitiveType.PrimitiveTypeName.INT32;
  }

  public Encoding getEncoding() {
    return dataBlock.getEncoding();
  }

  public Class getColumnClass() {
    return columnClass;
  }
}
