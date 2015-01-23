package com.intel.picklepot.column;

public class Settings {
  public static boolean enableDict = false;
  public static int dictSizeThreshold;
  public static int initialColSize;

  static {
    config(true, 5 * 1024 * 1024, 1024 * 1024);
  }

  public static void config(boolean enableDict, int dictBlockSizeThreshold, int initialSizePerCol) {
    Settings.enableDict = enableDict;
    Settings.dictSizeThreshold = dictBlockSizeThreshold;
    Settings.initialColSize = initialSizePerCol;
  }
}
