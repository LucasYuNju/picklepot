package com.intel.picklepot.column;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;
import parquet.column.values.deltastrings.DeltaByteArrayWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.io.api.Binary;

public class StringColumnWriter extends ColumnWriter {

  public StringColumnWriter(DataOutput output) {
    super(output);
    if (Settings.enableDict) {
      this.valuesWriter = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(Settings.dictSizeThreshold, Settings.initialColSize);
    }
    else {
      this.valuesWriter = new DeltaByteArrayWriter(Settings.initialColSize);
    }
  }

  @Override
  public void write(Object value) throws PicklePotException {
    numValues++;
    valuesWriter.writeBytes(Binary.fromByteArray(((String) value).getBytes()));
    statistics.add(value);
  }
}
