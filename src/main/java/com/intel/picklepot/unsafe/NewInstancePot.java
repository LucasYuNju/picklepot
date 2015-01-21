//package com.intel.picklepot.unsafe;
//
//import com.intel.picklepot.exception.PicklePotException;
//import com.intel.picklepot.storage.DataInput;
//import com.intel.picklepot.storage.DataOutput;
//import com.intel.picklepot.storage.SimpleDataInput;
//import com.intel.picklepot.storage.SimpleDataOutput;
//
//public class NewInstancePot<T> {
//  private FieldGroup fieldGroup;
//  private SimpleDataInput input;
//  private SimpleDataOutput output;
//
//  public NewInstancePot(DataOutput output) {
//    this.output = (SimpleDataOutput) output;
//  }
//
//  public NewInstancePot(DataInput input) {
//    this.input = (SimpleDataInput) input;
//    //TODO need extra initialization here?
//    this.fieldGroup = ((SimpleDataInput) input).readFieldGroup();
//  }
//
//  public FieldGroup getFieldGroup() {
//    return fieldGroup;
//  }
//
//  public void addObjectValue(T obj) throws PicklePotException {
//    if(!fieldGroup == null) {
//      this.fieldGroup = new FieldGroup(obj);
//    }
//    writeObject(obj);
//  }
//
//  private void writeObject(T obj) {
//
//  }
//}
