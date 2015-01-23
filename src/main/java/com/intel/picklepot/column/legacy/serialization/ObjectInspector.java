package com.intel.picklepot.column.legacy.serialization;

import com.intel.picklepot.exception.PicklePotException;

import java.util.List;

public interface ObjectInspector<T> {

  public void inspect(T obj, List<List<Object>> fieldCube) throws PicklePotException;
}
