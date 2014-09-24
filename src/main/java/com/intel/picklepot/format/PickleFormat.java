package com.intel.picklepot.format;

import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;

/**
 * PickleFormat define the format of serialized binary data, which mainly including class info,
 * field info and columnar serialized data.
 */
public interface PickleFormat {

  public void read(DataInput input);

  public void write(DataOutput output);
}
