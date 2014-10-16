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
    kryo.writeObject(output, pairs.size());
    for(Pair p : pairs) {
      kryo.writeObject(output, p);
    }
    output.flush();
    output.close();
    serialized = outputStream.toByteArray();
  }

  @Override
  protected void deserialize() throws Exception {
    deserialized = new LinkedList<Pair>();
    Kryo kryo = new Kryo();
    Input input = new Input(new ByteArrayInputStream(serialized));
    int size = kryo.readObject(input, Integer.class);
    for(int i=0; i<size; i++) {
      deserialized.add(kryo.readObject(input, Pair.class));
    }
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
//    List<Pair> pairs = InputUtils.getPairs();
//    for(int i=0; i<pairs.size(); i++) {
//      if(!deserialized.get(i).equals(pairs.get(i))) {
//        return false;
//      }
//    }
    return true;
  }
}
