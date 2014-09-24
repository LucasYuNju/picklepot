package com.intel.picklepot;

import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;

import java.util.Iterator;

/**
 * PicklePot is used to do batch serialization and deserialization.
 * @param <T> the object type which PicklePot instance can serialize/deserialize.
 */
public interface PicklePot<T> {

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
   * serialize all objects in cache into output.
   * @param output
   */
  public void serialize(DataOutput output);

  /**
   * deserialize an object from input.
   * @param input
   * @return
   */
  public T deserialize(DataInput input);
}
