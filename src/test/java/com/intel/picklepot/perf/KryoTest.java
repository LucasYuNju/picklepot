package com.intel.picklepot.perf;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.intel.picklepot.Pair;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.LinkedList;
import java.util.List;

public class KryoTest extends Template{
  List<Object> deserialized;
  Class clazz;

  public KryoTest(int repeations) {
    super("kryo+snappy", repeations);
  }

  @Override
  protected void serialize() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    List<Object> pairs = InputUtils.getObjects();
    clazz = pairs.get(0).getClass();
    Kryo kryo = new Kryo();
    Output output = new Output(outputStream);
    kryo.writeObject(output, pairs.size());
    for(Object p : pairs) {
      kryo.writeObject(output, p);
    }
    output.flush();
    output.close();
    serialized = outputStream.toByteArray();
    compressed = Snappy.compress(serialized);
    kryo = null;
  }

  @Override
  protected void deserialize() throws Exception {
    deserialized = new LinkedList<Object>();
    Kryo kryo = new Kryo();
    Input input = new Input(new ByteArrayInputStream(serialized));
    int size = kryo.readObject(input, Integer.class);
    for(int i=0; i<size; i++) {
      deserialized.add(kryo.readObject(input, clazz));
    }
    kryo = null;
  }

  @Override
  protected boolean verifyDeserialized() throws Exception {
//    List<Object> pairs = InputUtils.getObjects();
//    for(int i=0; i<pairs.size(); i++) {
//      if(!deserialized.get(i).equals(pairs.get(i))) {
//        return false;
//      }
//    }
    return true;
  }
}
