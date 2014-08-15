package com.intel.picklepot;

import com.intel.picklepot.storage.Storage;

public class Picklepot {

  public void writeObject(Storage storage, Object obj) {
    storage.add(obj);
  }

  public <T> T readObject (Storage storage, Class<T> type) {
    return null;
  }
}
