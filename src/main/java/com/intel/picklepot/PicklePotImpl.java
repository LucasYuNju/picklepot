package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.io.DataInput;
import com.intel.picklepot.io.DataOutput;
import com.intel.picklepot.io.SimpleDataInput;
import com.intel.picklepot.io.SimpleDataOutput;
import com.intel.picklepot.serialization.FieldGroup;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PicklePotImpl<T> implements PicklePot<T>{
  private volatile long count = 0;
  private FieldGroup fieldGroup;
  private DataInput input;
  private DataOutput output;
  private Map<Class, ObjectInstantiator> instantiators;

  public PicklePotImpl(OutputStream os, Map configuration) {
    output = new SimpleDataOutput(os);
  }

  public PicklePotImpl(InputStream is) {
    input = new SimpleDataInput(is);
    fieldGroup = input.readFieldGroup();
    count = fieldGroup.getNumVals();
    fieldGroup.setPicklePot(this);
    instantiators = new HashMap<Class, ObjectInstantiator>();
    instantiators.put(fieldGroup.getClazz(), new ObjenesisStd().getInstantiatorOf(fieldGroup.getClazz()));
  }

  @Override
  public void initialize(Class<T> className, DataOutput output, Map<String, String> configuration) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long write(T obj) throws PicklePotException {
    if (obj == null) {
      throw new PicklePotException("Null object added");
    }
    if(fieldGroup == null) {
      fieldGroup = new FieldGroup(obj, this);
    }
    fieldGroup.write(obj);
    return ++count;
  }

  @Override
  public long write(Iterator<T> ite) throws PicklePotException {
    while(ite.hasNext()) {
      write(ite.next());
    }
    return count;
  }

  @Override
  public void flush() throws PicklePotException {
    fieldGroup.setNumvals(count);
    output.writeFieldGroup(fieldGroup);
    fieldGroup.flush();
    output.flush();
  }

  @Override
  public T read() throws PicklePotException{
    if(count-- < 0) {
      return null;
    }
    if(input == null) {
      throw new PicklePotException("not initialized");
    }
    if(fieldGroup.isNested()) {
      Object obj = instantiators.get(fieldGroup.getClazz()).newInstance();
      fieldGroup.read(obj);
      return (T) obj;
    }
    else {
      return (T) fieldGroup.read();
    }
  }

  @Override
  public void close() {
    count = 0;
    output.close();
  }

  public boolean hasNext() {
    return count > 0;
  }

  public Object instantiate(Class clazz) {
    if(!instantiators.containsKey(clazz)) {
      instantiators.put(clazz, new ObjenesisStd().getInstantiatorOf(clazz));
    }
    return instantiators.get(clazz).newInstance();
  }

  public DataInput getInput() {
    return input;
  }

  public DataOutput getOutput() {
    return output;
  }
}
