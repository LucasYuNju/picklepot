package com.intel.picklepot.io;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldGroup;

public interface DataInput {

  /**
   * read class metadata
   */
  public FieldGroup readFieldGroup() throws PicklePotException;

  /**
   * @return column data read from internal stream
   */
  public Block readBlock() throws PicklePotException;

  /**
   * close DataInput, release all resources.
   */
  public void close() throws PicklePotException;
}
