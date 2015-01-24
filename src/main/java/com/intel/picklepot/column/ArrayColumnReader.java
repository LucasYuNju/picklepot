package com.intel.picklepot.column;

import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.serialization.Type;

public class ArrayColumnReader extends ColumnReader {
  private ColumnReader lengthReader;
  private ColumnReader compReader;

  public ArrayColumnReader(DataInput input, Class clazz) {
    this.lengthReader = new IntColumnReader(input);
    Class compClazz = clazz.getComponentType();
    switch (Type.get(compClazz)) {
      case STRING:
        compReader = new StringColumnReader(input);
        break;
      case INT:
        compReader = new IntColumnReader(input);
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public Object read() {
    Integer len = (Integer) lengthReader.read();
    Object[] ret = new Object[len];
    for(int i=0; i<len; i++) {
      ret[i] = compReader.read();
    }
    return ret;
  }
}
