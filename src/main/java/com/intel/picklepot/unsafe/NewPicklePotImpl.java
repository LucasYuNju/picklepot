package com.intel.picklepot.unsafe;

import com.intel.picklepot.PicklePot;
import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.storage.DataInput;
import com.intel.picklepot.storage.DataOutput;
import com.intel.picklepot.storage.SimpleDataInput;
import com.intel.picklepot.storage.SimpleDataOutput;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.objenesis.instantiator.ObjectInstantiator;

import java.util.*;

public class NewPicklePotImpl<T> implements PicklePot<T>{
  private volatile long count = 0;
  private FieldGroup fieldGroup;
  private SimpleDataInput input;
  private SimpleDataOutput output;

  @Override
  public void initialize(Class className, DataOutput output, Map configuration) {
    this.output = (SimpleDataOutput) output;
  }

  @Override
  public long add(T obj) throws PicklePotException {
    if (obj == null) {
      throw new PicklePotException("Null object added");
    }
    if(fieldGroup == null) {
      this.fieldGroup = new FieldGroup(obj, this);
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

  /**
   * do the compression work and flush to DataOutput.
   */
  public void flush() throws PicklePotException {
    output.initialize();
    fieldGroup.setNumvals(count);
    output.writeFieldGroup(fieldGroup);
    fieldGroup.flush();
  }

  public void close() throws PicklePotException{
    output.close();
  }

  @Override
  public Iterator<T> deserialize(DataInput input) throws PicklePotException {
    this.input = (SimpleDataInput) input;
    this.fieldGroup = this.input.readFieldGroup();
    this.count = fieldGroup.getNumVals();
    fieldGroup.setPicklePot(this);

    List<Object> res = new ArrayList<Object>((int)count);
    Class clazz = fieldGroup.getClazz();
    Objenesis objenesis = new ObjenesisStd();
    ObjectInstantiator instantiator = objenesis.getInstantiatorOf(clazz);
    for( ; count>0; count--) {
      if(fieldGroup.isNested()) {
        Object obj = instantiator.newInstance();
        fieldGroup.read(obj);
        res.add(obj);
      }
      else {
        res.add(fieldGroup.read());
      }
    }
    return (Iterator<T>) res.iterator();
  }

  //TODO need a sequential fetch method
  SimpleDataInput getInput() {
    return input;
  }

  SimpleDataOutput getOutput() {
    return output;
  }
}
