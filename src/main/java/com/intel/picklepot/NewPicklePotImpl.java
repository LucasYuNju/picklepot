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

public class NewPicklePotImpl<T> implements PicklePot<T>{
  private volatile long count = 0;
  private FieldGroup fieldGroup;
  private SimpleDataInput input;
  private SimpleDataOutput output;
  private Map<Class, ObjectInstantiator> instantiators;

  public NewPicklePotImpl(OutputStream os, Map configuration) {
    output = new SimpleDataOutput(os);
    output.initialize();
  }

  public NewPicklePotImpl(InputStream is) {
    input = new SimpleDataInput();
    input.initialize(is);
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
  public long add(T obj) throws PicklePotException {
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
  public long add(Iterator<T> ite) throws PicklePotException {
    while(ite.hasNext()) {
      T obj = ite.next();
      add(obj);
    }
    return count;
  }

  @Override
  public void flush() throws PicklePotException {
    fieldGroup.setNumvals(count);
    output.writeFieldGroup(fieldGroup);
    fieldGroup.flush();
  }

  @Override
  public Iterator<T> deserialize(DataInput input) throws PicklePotException {
    throw new UnsupportedOperationException();
  }

  @Override
  public T deserialize() throws PicklePotException{
    if(count-- < 0) {
      return null;
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

  public SimpleDataInput getInput() {
    return input;
  }

  public SimpleDataOutput getOutput() {
    return output;
  }
}
