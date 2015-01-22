package com.intel.picklepot.unsafe;

import com.intel.picklepot.PicklePot;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;

public class NewPicklePotImpl<T> implements PicklePot<T>{
  private volatile long count = 0;
  private FieldGroup fieldGroup;
  private SimpleDataInput input;
  private SimpleDataOutput output;
  private ObjectInstantiator instantiator;

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
    instantiator = new ObjenesisStd().getInstantiatorOf(fieldGroup.getClazz());
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
      Object obj = instantiator.newInstance();
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

  Object instantiate(Class clazz) {
    ObjectInstantiator inst = new ObjenesisStd().getInstantiatorOf(clazz);
    return inst.newInstance();
  }

  SimpleDataInput getInput() {
    return input;
  }

  SimpleDataOutput getOutput() {
    return output;
  }
}
