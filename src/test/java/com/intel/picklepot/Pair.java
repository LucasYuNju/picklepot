package com.intel.picklepot;

import java.io.Serializable;

//implements Serializabel for java native serialization
public class Pair implements Serializable{
  public String word;
  public int count;

  //default constructor for kryo serialization
  public Pair() {

  }

  public Pair(String word, int count) {
    this.word = word;
    this.count = count;
  }

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof Pair) {
      Pair p = (Pair) obj;
      return word.equals(p.word) && count == p.count;
    }
    return false;
  }
}