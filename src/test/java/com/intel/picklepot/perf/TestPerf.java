package com.intel.picklepot.perf;

import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class TestPerf {
  static final int REPETITION = 4;

  @Test
  public void test() throws Exception {
    InputUtils.getPairs();
    List<Template> tests = new LinkedList<Template>();
    tests.add(new PicklePotTest(REPETITION));
    tests.add(new NativeTest(REPETITION));
    tests.add(new KryoTest(REPETITION));
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
