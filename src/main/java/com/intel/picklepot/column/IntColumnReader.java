package com.intel.picklepot.column;

import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.serialization.Type;
import parquet.column.ColumnDescriptor;
import parquet.column.Encoding;
import parquet.column.ValuesType;

import java.io.IOException;

public class IntColumnReader extends ColumnReader{
  public IntColumnReader(DataInput input) {
    this.dataBlock = input.readBlock();
    Encoding encoding = dataBlock.getEncoding();
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {""}, Type.INT.toParquetType(), 0, 0);
    this.valuesReader = encoding.getValuesReader(descriptor, ValuesType.VALUES);
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
    return valuesReader.readInteger();
  }
}
