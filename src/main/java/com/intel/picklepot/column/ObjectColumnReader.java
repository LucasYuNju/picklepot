package com.intel.picklepot.column;

import com.intel.picklepot.column.values.ObjectValuesReader;
import com.intel.picklepot.io.DataInput;

import java.io.IOException;

public class ObjectColumnReader extends ColumnReader{
  public ObjectColumnReader(DataInput input) {
    this.dataBlock = input.readBlock();
    this.valuesReader = new ObjectValuesReader();
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
    return ((ObjectValuesReader) valuesReader).readObject();
  }
}
