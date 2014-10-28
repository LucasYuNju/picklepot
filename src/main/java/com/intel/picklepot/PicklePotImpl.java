package com.intel.picklepot;

import com.intel.picklepot.columnar.*;
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

    StopWatch.start();
    while(ite.hasNext()) {
      Object obj = ite.next();
      this.instancePot.addObjectValue(obj);
      count++;
    }
    StopWatch.stop("InstancePot.addObjectValue");

    return count;
  }

  @Override
  public Iterator<T> deserialize(DataInput input) throws PicklePotException {
    SimpleDataInput dataInput = (SimpleDataInput) input;

    try {
      dataInput.readObjects();
    } catch (Exception e) {
      throw new PicklePotException(e);
    }
    List<byte[]> fieldBytesList = dataInput.getFieldsByte();
    List<Iterator> fieldValueIterators = new LinkedList<Iterator>();
    Iterator<FieldInfo> fieldInfos = dataInput.getClassInfo().getFieldInfos().values().iterator();
    for(int i=0; fieldInfos.hasNext(); i++) {
      FieldInfo curFieldInfo = fieldInfos.next();
      Decoder decoder = getDecoder(curFieldInfo.getFieldType().toString());
      Iterator iterator = decoder.decode(fieldBytesList.get(i),curFieldInfo.getFieldType().toString());
      fieldValueIterators.add(iterator);
    }

    Class<T> clazz = dataInput.getClassInfo().getClassIns();
    Field[] fields = clazz.getFields();
    for(Field field : fields) {
      field.setAccessible(true);
    }

    Objenesis objenesis = new ObjenesisStd();
    ObjectInstantiator instantiator = objenesis.getInstantiatorOf(clazz);
    List<T> result = new LinkedList<T>();
    if(!InstancePot.isUnsupportedInstance(clazz)) {
      while (!fieldValueIterators.isEmpty() && fieldValueIterators.get(0).hasNext()) {
        try {
          T object = (T) instantiator.newInstance();
          for (int i = 0; i < fields.length; i++) {
            fields[i].set(object, fieldValueIterators.get(i).next());
          }
          result.add(object);
        } catch (IllegalAccessException e) {
          throw new PicklePotException(e);
        }
      }
    }
    else {
      Iterator instances = fieldValueIterators.get(0);
      while(instances.hasNext())
        result.add((T) instances.next());
    }
    return result.iterator();
  }

  /**
   * do the compression work and flush to DataOutput.
   */
  public void flush() throws PicklePotException {
    SimpleDataOutput dataOutput = (SimpleDataOutput) this.dataOutput;
    try {
      dataOutput.writeClassInfo(instancePot.getClassInfo());
    } catch (IOException e) {
      throw new PicklePotException();
    }
    Iterator<FieldInfo> fieldInfos = instancePot.getClassInfo().getFieldInfos().values().iterator();
    while(fieldInfos.hasNext()) {
      FieldInfo curFieldInfo = fieldInfos.next();
      List<?> list = instancePot.getFieldValues(curFieldInfo.getFieldName());
      Iterator iterator = list.iterator();

      try {
        Encoder encoder = getEncoder(curFieldInfo.getFieldType().toString());
        encoder.encode(iterator);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) encoder.getOutputStream();
        dataOutput.writeFieldByte(outputStream.toByteArray());
      } catch (IOException e) {
        throw new PicklePotException(e);
      }
    }
    dataOutput.close();
  }

  // just used for unit test.
  public InstancePot getInstancePot() {
    return instancePot;
  }

  /**
   * TODO
   * default encoding policy. this method may be placed elsewhere
   */
  private Encoder<T> getEncoder(String classToEncode) {
    classToEncode = classToEncode.replace("class", "").trim();
    if(Integer.class.getName().equals(classToEncode) || classToEncode.equals("int")) {
      return new RunLengthEncoder();
    }
    else if(String.class.getName().equals(classToEncode)){
      return new LZ4Encoder();
    }
    else {
      return new JavaEncoder<T>();
    }
  }

  /**
   * TODO
   * default decoding policy. this method may be placed elsewhere
   */
  private Decoder getDecoder(String classToDecode) {
    classToDecode = classToDecode.replace("class", "").trim();
    if(Integer.class.getName().equals(classToDecode) || classToDecode.equals("int")) {
      return new RunLengthDecoder();
    }
    else if(String.class.getName().equals(classToDecode)) {
      return new LZ4Decoder();
    }
    else {
      return new JavaDecoder();
    }
  }
}
