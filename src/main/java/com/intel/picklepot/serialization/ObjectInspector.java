package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.util.Map;

public interface ObjectInspector<T> {

  public Map<String, Object> inspect(T obj) throws PicklePotException;
}
