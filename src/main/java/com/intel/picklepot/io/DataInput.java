package com.intel.picklepot.io;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldGroup;

public interface DataInput {

  /**
   * read class metadata
   */
  public FieldGroup readFieldGroup();

  /**
   * @return column data read from internal stream
   */
  public Block readBlock();

  /**
   * close DataInput, release all resources.
   */
  public void close();
}
