package com.intel.picklepot.perf;

import com.intel.picklepot.columnar.ColumnWriter;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class TestPerf {
  static final int REPETITION = 1;
  static final int WARNUP = 0;
  static final int RUN= 0;

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

    PicklePotTest ppt = new PicklePotTest(REPETITION);
    ColumnWriter.enableColumnStatics(true);
    ppt.test();
    ColumnWriter.enableColumnStatics(false);
    System.gc();
    System.out.println();

    while(!tests.isEmpty()) {
      Template t = tests.get(0);
      t.test();
      t.printStatistics();
      tests.remove(t);
    }
  }
}
