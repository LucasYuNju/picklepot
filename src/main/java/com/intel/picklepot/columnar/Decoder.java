package com.intel.picklepot.columnar;

import java.util.Iterator;

/**
 * An Decoder is in charge of decode bytes with columnar optimized decompression algorithm.
 * @param <T>
 */
public interface Decoder<T> {

  public Iterator<T> decode(byte[] bytes);

}
