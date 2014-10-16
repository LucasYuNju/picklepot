package com.intel.picklepot.perf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.intel.picklepot.Pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.List;

public class KryoTest extends Template{
  List<Pair> deserialized;

  public KryoTest(int repeations) {
    super("kryo\t", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    List<Pair> pairs = InputUtils.getPairs();
    Kryo kryo = new Kryo();
    Output output = new Output(outputStream);
    kryo.writeObject(output, pairs);
    output.close();
    serialized = outputStream.toByteArray();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void deserialize() throws Exception {
    deserialized = new LinkedList<Pair>();
    Kryo kryo = new Kryo();
    Input input = new Input(new ByteArrayInputStream(serialized));
    deserialized = kryo.readObject(input, LinkedList.class);
    input.close();
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
    List<Pair> pairs = InputUtils.getPairs();
    for(int i=0; i<pairs.size(); i++) {
      if(!deserialized.get(i).equals(pairs.get(i))) {
        return false;
      }
    }
    return true;
  }
}
