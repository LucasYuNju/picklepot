package com.intel.picklepot.column;

public class Settings {
  public static boolean enableDict = false;
  public static int dictSizeThreshold;
  /**
   * Better use small size. Parquet use CapacityByteArrayOutputStream to store serialized data,
   * which is much more efficient than java.io.ByteArrayOutputStream when increasing size.
   */
  public static int initialColSize;

  static {
    config(true, 256 * 1024, 64 * 1024);
  }

  public static void config(boolean enableDict, int dictBlockSizeThreshold, int initialSizePerCol) {
    Settings.enableDict = enableDict;
    Settings.dictSizeThreshold = dictBlockSizeThreshold;
    Settings.initialColSize = initialSizePerCol;
  }
}
