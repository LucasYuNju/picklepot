package com.intel.picklepot;

public class Pair {
  public String word;
  public int count;

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