package com.intel.picklepot;

import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;

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
   * add an object to cache for serialization.
   * @param obj
   * @return return current object count.
   */
  public long add(T obj);

  /**
   * add all objects in the iterator to cache for serialization.
   * @param ite
   * @return return current object count.
   */
  public long add(Iterator<T> ite);

  /**
   * deserialize an object from input.
   * @param input
   * @return
   */
  public Iterator<T> deserialize(DataInput input);
}
