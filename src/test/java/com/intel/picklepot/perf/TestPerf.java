package com.intel.picklepot.perf;

import java.util.LinkedList;
import java.util.List;

public class TestPerf {
  static final int REPETITION = 1;

  public static void main(String args[]) throws Exception {
    InputUtils.getPairs();
    List<Template> tests = new LinkedList<Template>();
    tests.add(new PicklePotTest(REPETITION));
    tests.add(new NativeTest(REPETITION));
    tests.add(new KryoTest(REPETITION));
    Runtime.getRuntime().gc();
    for(Template t : tests) {
      for(int i=0; i<1; i++) {
        t.test();
        t.printStatistics();
        System.gc();
      }
    }
  }
}
