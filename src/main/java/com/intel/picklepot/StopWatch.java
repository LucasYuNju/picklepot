package com.intel.picklepot;

public class StopWatch {
  private static long startTimeNanos;

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  public static void start() {
    startTimeNanos = System.nanoTime();
  }

  /**
   * @return nano time since last start
   */
  public static void stop(String info) {
    long time = System.nanoTime() - startTimeNanos;
    System.out.printf(info + ":%,dms\n", time / 1000000);
  }
}
