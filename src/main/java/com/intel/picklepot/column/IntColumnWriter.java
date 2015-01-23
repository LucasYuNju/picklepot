package com.intel.picklepot.column;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;
import parquet.column.values.delta.DeltaBinaryPackingValuesWriter;

public class IntColumnWriter extends ColumnWriter {

  public IntColumnWriter(DataOutput output) {
    super(output);
    this.valuesWriter = new DeltaBinaryPackingValuesWriter(Settings.initialColSize);
  }

  @Override
  public void write(Object value) throws PicklePotException {
    numValues++;
    valuesWriter.writeInteger((Integer) value);
    statistics.add(value);
  }
}
