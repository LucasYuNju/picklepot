package com.intel.picklepot.columnar;

/**
 * An Decoder is in charge of decode byte array into array values with columnar optimized
 * decompression algorithm.
 * @param <T>
 */
public interface Decoder<T> {

  public T[] decode(byte[] bytes);

}
