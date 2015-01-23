
package com.intel.picklepot.io;

import com.intel.picklepot.format.Block;
import com.intel.picklepot.serialization.FieldGroup;

public interface DataOutput {

  /**
   * write class metadata to internal storage.
   */
  public void writeFieldGroup(FieldGroup fieldGroup);

  /**
   * write column data to internal storage.
   * @param dataBlock
   */
  public void writeBlock(Block dataBlock);

  public void flush();

  public void close();
}