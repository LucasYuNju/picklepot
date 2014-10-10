package com.intel.picklepot.columnar;

import java.util.Iterator;

/**
 * An Decoder is in charge of decode bytes with columnar optimized decompression algorithm.
 * @param <T>
 */
public interface Decoder<T> {
    /**
     * restore compressed bytes to object collection
     * @param bytes compressed bytes
     * @param className class name of target object
     * @return iterator of object collection
     */
    public Iterator<T> decode(byte[] bytes, int numObject, String className);

}
