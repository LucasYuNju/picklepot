package com.intel.picklepot.unsafe;

import com.intel.picklepot.PicklePot;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class NewPicklePotImpl<T> implements PicklePot<T>{
  private Map<Class, FieldGroup> groups = new HashMap<Class, FieldGroup>();
  private volatile long count = 0;

  @Override
  public void initialize(Class className, DataOutput output, Map configuration) {

  }

  @Override
  public long add(Object obj) throws PicklePotException {
    if (obj == null) {
      throw new PicklePotException("Null object added");
    }
    this.instancePot.addObjectValue(obj);
    return ++count;
  }

  @Override
  public long add(Iterator ite) throws PicklePotException {
    if (ite == null) {
      throw new PicklePotException("Null object added");
    }

    while(ite.hasNext()) {
      T obj = ite.next();
      this.instancePot.addObjectValue(obj);
      count++;
    }

    return count;
  }

  @Override
  public Iterator deserialize(DataInput input) throws PicklePotException {
    return null;
  }
}
