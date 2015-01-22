package com.intel.picklepot.perf;

import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.io.SimpleDataInput;
import com.intel.picklepot.io.SimpleDataOutput;
import org.xerial.snappy.Snappy;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class PicklePotTest extends Template {
  PicklePotImpl<Object> picklePot;
  Iterator restored;
  List objects;

  public PicklePotTest(int repeations) {
    super("picklepot+snappy", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    picklePot = new PicklePotImpl<Object>();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    objects = InputUtils.getObjects();
    picklePot.initialize((Class)objects.get(0).getClass(), new SimpleDataOutput(outputStream), null);
    picklePot.add(objects.iterator());

    picklePot.flush();
    picklePot.close();
    serialized = outputStream.toByteArray();
    compressed = Snappy.compress(serialized);
  }

  @Override
  protected void deserialize() throws Exception {
    SimpleDataInput dataInput = new SimpleDataInput();
    dataInput.initialize(new ByteArrayInputStream(serialized));
    restored = picklePot.deserialize(dataInput);
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
