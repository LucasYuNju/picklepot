package com.intel.picklepot.columnar;

/**
 * An Encoder is in charge of encode array values into byte array with columnar optimized
 * compression algorithm.
 * @param <T>
 */
public interface Encoder<T> {

  public byte[] encode(T[] values);

}
