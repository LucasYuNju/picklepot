package com.intel.picklepot.perf;

import com.intel.picklepot.Pair;
import com.intel.picklepot.PicklePotImpl;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import com.intel.picklepot.StopWatch;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class PicklePotTest extends Template {
  PicklePotImpl<Pair> picklePot;
  Iterator deserialized;

  public PicklePotTest(int repeations) {
    super("picklepot", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    picklePot = new PicklePotImpl<Pair>();
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    picklePot.initialize(Pair.class, new SimpleDataOutput(outputStream), null);
    List<Pair> pairs = InputUtils.getPairs();
    picklePot.add(pairs.iterator());

    picklePot.flush();
    serialized = outputStream.toByteArray();
  }

  @Override
  protected void deserialize() throws Exception {
    SimpleDataInput dataInput = new SimpleDataInput();
    dataInput.initialize(new ByteArrayInputStream(serialized));
    deserialized = picklePot.deserialize(dataInput);
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    List<Pair> pairs = InputUtils.getPairs();
    for(Pair p : pairs) {
      if(!deserialized.hasNext() || !deserialized.next().equals(p))
        return false;
    }
    return true;
  }
}
