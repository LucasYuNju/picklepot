package com.intel.picklepot.perf;

import java.util.LinkedList;
import java.util.List;

//kryo serialize list directly
public class TestPerf {
  static final int REPEATION = 4;

  public static void main(String args[]) throws Exception {
    InputUtils.getPairs();
    List<Template> tests = new LinkedList<Template>();
    tests.add(new PicklePotTest(REPEATION));
    tests.add(new NativeTest(REPEATION));
    tests.add(new KryoTest(REPEATION));
    Runtime.getRuntime().gc();
    for(Template t : tests) {
      for(int i=0; i<3; i++) {
        t.test();
        t.printStatistics();
        System.gc();
      }
    }
  }
}
