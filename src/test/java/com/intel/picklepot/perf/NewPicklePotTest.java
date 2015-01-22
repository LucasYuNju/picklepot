package com.intel.picklepot.perf;

import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import com.intel.picklepot.unsafe.NewPicklePotImpl;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class NewPicklePotTest extends Template{
  NewPicklePotImpl<Object> picklePot;
  Iterator restored;
  List objects;

  public NewPicklePotTest(int repeations) {
    super("newpicklepot+snappy", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    picklePot = new NewPicklePotImpl<Object>(outputStream, null);

    objects = InputUtils.getObjects();
    picklePot.add(objects.iterator());
    picklePot.flush();
    picklePot.close();
    serialized = outputStream.toByteArray();
    compressed = Snappy.compress(serialized);
  }

  @Override
  protected void deserialize() throws Exception {
    picklePot = new NewPicklePotImpl<Object>(new ByteArrayInputStream(serialized));
    LinkedList<Object> list = new LinkedList<Object>();
    while(picklePot.hasNext()) {
      list.add(picklePot.deserialize());
    }
    restored = list.iterator();
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    for(Object p : objects) {
      if(!restored.hasNext() || !restored.next().equals(p))
        return false;
    }
    return true;
  }
}
