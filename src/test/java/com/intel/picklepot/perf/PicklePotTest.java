package com.intel.picklepot.perf;

import com.intel.picklepot.PicklePotImpl;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class PicklePotTest extends Template{
  PicklePotImpl<Object> picklePot;
  Iterator restored;
  List objects;

  public PicklePotTest(int repeations) {
    super("picklepot+snappy", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    picklePot = new PicklePotImpl<Object>(outputStream, null);

    objects = InputUtils.getObjects();
    for(Object obj : objects) {
      picklePot.write(obj);
    }
    picklePot.flush();
    picklePot.close();
    serialized = outputStream.toByteArray();
    compressed = Snappy.compress(serialized);
    picklePot = null;
  }

  @Override
  protected void deserialize() throws Exception {
    picklePot = new PicklePotImpl<Object>(new ByteArrayInputStream(serialized));
    LinkedList<Object> list = new LinkedList<Object>();
    while(picklePot.hasNext()) {
      list.add(picklePot.read());
    }
    restored = list.iterator();
    picklePot = null;
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    for(Object p : objects) {
      if(!restored.hasNext() || !restored.next().equals(p))
        return false;
    }
    restored = null;
    return true;
  }
}
