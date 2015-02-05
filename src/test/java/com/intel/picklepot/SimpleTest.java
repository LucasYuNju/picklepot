package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import com.intel.picklepot.serialization.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SerializableWritable;
import org.apache.spark.util.collection.CompactBuffer;
import scala.Tuple2;

import java.io.*;
import java.lang.reflect.Field;

public class SimpleTest<T> implements Serializable{
  long l;
  float f;
  double d;
  String s;
  Integer[] a;
  T t;

  public void init(int val, T t) {
    l = val;
    f = val + 0.1f;
    d = f;
    s = "a" + val;
    a = new Integer[]{val, val};
    this.t = t;
  }

  public static void testPiclePot() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    SimpleTest<Object> obj = new SimpleTest<Object>();
    obj.init(1, new Pair("a", 123));
    PicklePot<Object> picklePot = new PicklePotImpl<Object>(baos, null);
    picklePot.write(obj);
    picklePot.write(obj);
    picklePot.flush();
    picklePot.close();

    System.out.println(baos.toByteArray().length);

    byte[] bytes = new byte[baos.toByteArray().length * 2];
    System.arraycopy(baos.toByteArray(), 0, bytes, 0, baos.toByteArray().length);

    PicklePot picklepot = new PicklePotImpl(new ByteArrayInputStream(bytes));
    while(picklepot.hasNext()) {
      Object restored = picklepot.read();
      System.out.println(restored);
    }

    System.out.println(picklepot.toString());
  }

  public static void testNonNested() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePotImpl<Object> picklePot = new PicklePotImpl<Object>(baos, null);
    Object write;
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.s3.awsAccessKeyId", "AWS_ACCESS_KEY_ID");
    write = new SerializableWritable(hadoopConf);

    picklePot.write(write);
    picklePot.write(write);
    picklePot.flush();
    picklePot.close();

    PicklePotImpl picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Object obj = picklepot.read();
    System.out.println(obj);
    obj = picklepot.read();
    System.out.println(obj);
  }


  public static void main(String args[]) throws Exception {
    testPiclePot();
//    testNonNested();
  }
}
