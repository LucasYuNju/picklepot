package com.intel.picklepot;

import com.intel.picklepot.columnar.Decoder;
import com.intel.picklepot.columnar.Encoder;
import com.intel.picklepot.columnar.LZ4Decoder;
import com.intel.picklepot.columnar.LZ4Encoder;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.metadata.FieldInfo;
import com.intel.picklepot.serialization.InstancePot;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * PicklePotImpl is not thread-safe, so do not use it in multi-threads.
 * @param <T>
 */
public class PicklePotImpl<T> implements PicklePot<T>{
  private Class<T> className;
  private DataOutput dataOutput;
  private Map<String, String> configuration;
  private InstancePot<T> instancePot;
  private volatile long count = 0;
  private Encoder<T> lz4Encoder = new LZ4Encoder();
  private Decoder<T> lz4Decoder = new LZ4Decoder();

  @Override
  public void initialize(Class<T> className, DataOutput output, Map<String, String> configuration) {
    this.className = className;
    this.dataOutput = output;
    this.configuration = configuration;
    this.instancePot = new InstancePot<T>(this.className);
  }

  @Override
  public long add(T obj) throws PicklePotException {
    if (obj == null) {
      return count;
    }
    this.instancePot.addObjectValue(obj);
    return ++count;
  }

  @Override
  public long add(Iterator<T> ite) throws PicklePotException {
    if (ite == null) {
      return count;
    }

    while(ite.hasNext()) {
      Object obj = ite.next();
      this.instancePot.addObjectValue(obj);
      count++;
    }

    return count;
  }

  @Override
  public Iterator<T> deserialize(DataInput input) throws PicklePotException, IOException, ClassNotFoundException {
    SimpleDataInput dataInput = (SimpleDataInput) input;
    dataInput.readObjects();
    List<byte[]> fieldBytesList = dataInput.getFieldsByte();
    List<Iterator> fieldValueIterators = new LinkedList<Iterator>();
    Iterator<FieldInfo> fieldInfos = instancePot.getClassInfo().getFieldInfos().values().iterator();
    for(int i=0; fieldInfos.hasNext(); i++) {
      Iterator iterator = lz4Decoder.decode(fieldBytesList.get(i), fieldInfos.next().getFieldType().toString());
      fieldValueIterators.add(iterator);
    }

    Class<T> clazz = instancePot.getClassInfo().getClassIns();
    Field[] fields = clazz.getFields();
    for(Field field : fields) {
      field.setAccessible(true);
    }

    Objenesis objenesis = new ObjenesisStd();
    ObjectInstantiator instantiator = objenesis.getInstantiatorOf(clazz);
    List<T> result = new LinkedList<T>();
    while(!fieldValueIterators.isEmpty() && fieldValueIterators.get(0).hasNext()) {
      try {
        T object = (T) instantiator.newInstance();
        for(int i=0; i<fields.length; i++) {
          fields[i].set(object, fieldValueIterators.get(fields.length - 1 - i).next());
        }
        result.add(object);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return result.iterator();
  }

  /**
   * do the compression work and flush to DataOutput.
   */
  public void flush() throws IOException, PicklePotException {
    SimpleDataOutput dataOutput = (SimpleDataOutput) this.dataOutput;
    dataOutput.writeClassInfo(instancePot.getClassInfo());
    Iterator<String> fieldNames = instancePot.getClassInfo().getFieldInfos().keySet().iterator();
    while(fieldNames.hasNext()) {
      List<Object> list = instancePot.getFieldValues(fieldNames.next());
        Iterator iterator = list.iterator();
        lz4Encoder.encode(iterator);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) lz4Encoder.getOutputStream();
        dataOutput.writeFieldByte(outputStream.toByteArray());
      }
    dataOutput.close();
  }

  // just used for unit test.
  public InstancePot<T> getInstancePot() {
    return instancePot;
  }
}
