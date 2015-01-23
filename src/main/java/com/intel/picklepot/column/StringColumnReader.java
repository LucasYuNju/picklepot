package com.intel.picklepot.column;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.serialization.Type;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.ValuesType;

import java.io.IOException;

public class StringColumnReader extends ColumnReader{
  public StringColumnReader(DataInput input) {
    this.dataBlock = input.readBlock();
    Block dictBlock = dataBlock.getEncoding().usesDictionary() ? input.readBlock() : null;
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, Type.STRING.getParquetType(), 0, 0);
    Encoding encoding = dataBlock.getEncoding();
    if (encoding.usesDictionary()) {
      Dictionary dict = getDictioinary(dictBlock, encoding, descriptor);
      valuesReader = encoding.getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dict);
    } else {
      valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
    }
    try {
      valuesReader.initFromPage(dataBlock.getNumValues(), dataBlock.getBytes(), 0);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public Object read() {
    if(!hasNext())
        return null;
    return new String(valuesReader.readBytes().getBytes());
  }
}
