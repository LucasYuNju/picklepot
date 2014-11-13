package com.intel.picklepot.perf;

import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TestPerf {
  static final int REPETITION = 4;
  static final int WARNUP = 4;
  static final int RUN= 5;

  @Test
  public void test() throws Exception {
    List<Template> tests = new LinkedList<Template>();
    tests.add(new NativeTest(REPETITION));
    tests.add(new PicklePotTest(REPETITION));
    tests.add(new KryoTest(REPETITION));
    Runtime.getRuntime().gc();

    for(int i=0; i<WARNUP; i++) {
      for (Template t : tests) {
        t.test();
      }
    }
    System.gc();

    for(Template t : tests) {
      for(int i=0; i<RUN; i++) {
        t.test();
        t.printStatistics();
      }
      System.gc();
    }
  }
}
