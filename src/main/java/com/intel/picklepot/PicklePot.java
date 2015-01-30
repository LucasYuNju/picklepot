package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataOutput;

import java.util.Iterator;
import java.util.Map;

/**
 * PicklePot is used to do batch serialization and deserialization.
 * @param <T> the object type which PicklePot instance can serialize/deserialize.
 */
public interface PicklePot<T> {

  /**
   * initialize PicklePot with certain class type and configurations.
   * @param className
   * @param configuration
   */
  public void initialize(Class<T> className, DataOutput output, Map<String, String> configuration);

  /**
   * serialize object, cache the serialized object in memory.
   * @param obj
   * @return return current object count.
   */
  public long write(T obj) throws PicklePotException;

  /**
   * serialize all objects in the iterator.
   * @param ite
   * @return return current object count.
   */
  public long write(Iterator<T> ite) throws PicklePotException;

  /**
   * deserialize an object from input.
   * @return null if there is no object left
   * @throws PicklePotException
   * @throws java.lang.UnsupportedOperationException
   */
  public T read() throws PicklePotException;

  /**
   * code columns and flush to DataOutput.
   */
  public void flush() throws PicklePotException;

  /**
   * close stream
   */
  public void close();

  public boolean hasNext();
}
