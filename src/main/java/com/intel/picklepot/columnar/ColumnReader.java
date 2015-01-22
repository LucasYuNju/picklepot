package com.intel.picklepot.columnar;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldType;
import parquet.column.Encoding;
import parquet.column.values.ValuesReader;

public class ColumnReader {
  private ValuesReader valuesReader;
  private Block dataBlock;
  private Class clazz;
  private FieldType type;
  private int numRead;

  public ColumnReader(ValuesReader valuesReader, Block dataBlock, Class columnClass) {
    this.clazz = columnClass;
    this.valuesReader = valuesReader;
    this.dataBlock = dataBlock;
    this.type = Utils.toFieldType(clazz);
  }

  public Object read() {
    if(!hasNext())
      return null;
    numRead++;
    if(type == FieldType.STRING)
      return new String(valuesReader.readBytes().getBytes());
    else if(type == FieldType.INT) {
      return valuesReader.readInteger();
    }
    else {
      return ((ObjectValuesReader) valuesReader).readObject();
    }
  }

  public boolean hasNext() {
    return numRead < dataBlock.getNumValues();
  }

  public Encoding getEncoding() {
    return dataBlock.getEncoding();
  }

  /**
   * for test
   */
  public Class getColumnClass() {
    return clazz;
  }
}
