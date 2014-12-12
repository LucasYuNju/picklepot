package com.intel.picklepot.columnar;

import com.intel.picklepot.storage.SimpleDataOutput;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;

public class PicklePotProperties {
  private boolean enableDict;
  private int dictBlockSizeThreshold;
  int initialSizePerCol;

  public PicklePotProperties(boolean enableDict, int dictBlockSizeThreshold, int initialSizePerCol) {
    this.enableDict = enableDict;
    this.dictBlockSizeThreshold = dictBlockSizeThreshold;
    this.initialSizePerCol = initialSizePerCol;
  }

  public ColumnWriter getColumnWriter(Class columnClazz, SimpleDataOutput output) {
    ValuesWriter valuesWriter;
    if(columnClazz == String.class) {
      if (enableDict) {
        valuesWriter = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(dictBlockSizeThreshold, initialSizePerCol);
      }
      else {
        valuesWriter = new DeltaByteArrayWriter(initialSizePerCol);
      }
    }
    else if(columnClazz == Integer.class || columnClazz == int.class) {
      valuesWriter = new DeltaBinaryPackingValuesWriter(initialSizePerCol);
    }
    else {
      valuesWriter = new ObjectValuesWriter();
    }
    return new ColumnWriter(valuesWriter, output);
  }
}
