package com.intel.picklepot.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.util.List;
import java.util.Map;

public interface ObjectInspector<T> {

  public void inspect(T obj, List<List<Object>> fieldCube) throws PicklePotException;
}
