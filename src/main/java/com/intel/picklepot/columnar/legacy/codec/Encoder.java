
package com.intel.picklepot.columnar.legacy.codec;

import java.io.OutputStream;
import java.util.Iterator;

/**
 * An Encoder is in charge of encode values into OutputStream with columnar optimized
 * compression algorithm.
 *
 * @param <T>
 */
public interface Encoder<T> {

  public OutputStream getOutputStream();

  public void encode(Iterator<T> values, int num);

  public void encode(T value);

}