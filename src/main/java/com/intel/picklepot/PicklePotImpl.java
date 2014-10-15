package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.serialization.InstancePot;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;

import java.util.Iterator;
import java.util.Map;

/**
 * PicklePotImpl is not thread-safe, so do not use it in multi-threads.
 * @param <T>
 */
public class PicklePotImpl<T> implements PicklePot<T>{
  private Class<T> className;
  private DataOutput dataOutput;
  private Map<String, String> configuration;
  private InstancePot<T> instancePot;
  private volatile long count = 0;

  @Override
  public void initialize(Class<T> className, DataOutput output, Map<String, String> configuration) {
    this.className = className;
    this.dataOutput = output;
    this.configuration = configuration;
    this.instancePot = new InstancePot<T>(this.className);
  }

  @Override
  public long add(T obj) throws PicklePotException {
    if (obj == null) {
      return count;
    }
    this.instancePot.addObjectValue(obj);
    return ++count;
  }

  @Override
  public long add(Iterator<T> ite) throws PicklePotException {
    if (ite == null) {
      return count;
    }

    while(ite.hasNext()) {
      Object obj = ite.next();
      this.instancePot.addObjectValue(obj);
      count++;
    }

    return count;
  }

  @Override
  public Iterator<T> deserialize(DataInput input) {
    return null;
  }

  /**
   * do the compression work and flush to DataOutput.
   */
  private void flush() {
    //TODO

  }

  // just used for unit test.
  public InstancePot<T> getInstancePot() {
    return instancePot;
  }
}
