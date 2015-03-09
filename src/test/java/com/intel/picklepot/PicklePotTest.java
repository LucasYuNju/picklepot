/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.picklepot;

import com.intel.picklepot.exception.PicklePotException;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Test;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PicklePotTest {
  @Test
  public void testInt() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Integer> picklePot = new PicklePotImpl<Integer>(baos, null);

    for (int i = 0; i < 10; i++) {
      picklePot.write(i);
    }

    picklePot.flush();
    picklePot.close();

    PicklePot<Integer> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    int[] results = new int[10];
    int i=0;
    while (picklepot.hasNext()) {
      results[i] = picklepot.read();
      i++;
    }

    int[] expects = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
    assertArrayEquals(expects, results);
  }

  @Test
  public void testLong() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Long> picklePot = new PicklePotImpl<Long>(baos, null);

    for (long i = 0; i < 10; i++) {
      picklePot.write(i);
    }

    picklePot.flush();
    picklePot.close();

    PicklePot<Long> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Long[] results = new Long[10];
    int i=0;
    while (picklepot.hasNext()) {
      results[i] = picklepot.read();
      i++;
    }

    Long[] expects = { 0l, 1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l };
    assertArrayEquals(expects, results);
  }

  @Test
  public void testBoolean() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Boolean> picklePot = new PicklePotImpl<Boolean>(baos, null);

    picklePot.write(true);
    picklePot.write(false);

    picklePot.flush();
    picklePot.close();
    //1800B

    PicklePot<Boolean> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    boolean result1 = picklepot.read();
    boolean result2 = picklepot.read();

    assertEquals(true, result1);
    assertEquals(false, result2);
  }

  @Test
  public void testFloat() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Float> picklePot = new PicklePotImpl<Float>(baos, null);

    for (int i = 0; i < 10; i++) {
      picklePot.write(i + 0.1f);
    }

    picklePot.flush();
    picklePot.close();

    PicklePot<Float> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Float[] results = new Float[10];
    int i=0;
    while (picklepot.hasNext()) {
      results[i] = picklepot.read();
      i++;
    }

    Float[] expects = { 0.1f, 1.1f, 2.1f, 3.1f, 4.1f, 5.1f, 6.1f, 7.1f, 8.1f, 9.1f };
    assertArrayEquals(expects, results);
  }

  @Test
  public void testDouble() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Double> picklePot = new PicklePotImpl<Double>(baos, null);

    for (long i = 0; i < 10; i++) {
      picklePot.write(i + 0.1d);
    }

    picklePot.flush();
    picklePot.close();

    PicklePot<Double> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Double[] results = new Double[10];
    int i=0;
    while (picklepot.hasNext()) {
      results[i] = picklepot.read();
      i++;
    }

    Double[] expects = { 0.1d, 1.1d, 2.1d, 3.1d, 4.1d, 5.1d, 6.1d, 7.1d, 8.1d, 9.1d };
    assertArrayEquals(expects, results);
  }

  @Test
  public void testString() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<String> picklePot = new PicklePotImpl<String>(baos, null);

    picklePot.write("hello");
    picklePot.write("helloworld");
    picklePot.write("world");

    picklePot.flush();
    picklePot.close();

    PicklePot<String> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    String[] results = new String[3];
    int i=0;
    while (picklepot.hasNext()) {
      results[i] = picklepot.read();
      i++;
    }

    String[] expects = { "hello", "helloworld", "world" };
    assertArrayEquals(expects, results);
  }

  @Test
  public void testArray() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<byte[]> picklePot = new PicklePotImpl<byte[]>(baos, null);

    picklePot.write("hello".getBytes());

    picklePot.flush();
    picklePot.close();

    PicklePot<byte[]> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    byte[] result = picklepot.read();

    byte[] expects = { 'h', 'e', 'l', 'l', 'o' };
    assertArrayEquals(expects, result);
  }

  @Test
  public void testTurple1() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    PicklePot<Tuple2<Integer, String>> picklePot = new PicklePotImpl<Tuple2<Integer, String>>(baos, null);

    picklePot.write(new Tuple2<Integer, String>(1, "intel"));

    picklePot.flush();
    picklePot.close();

    PicklePot<Tuple2<Integer, String>> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Tuple2<Integer, String> result = picklepot.read();

    assertEquals(1, result._1().intValue());
    assertEquals("intel", result._2());
  }

  @Test
  public void testTurple2() throws PicklePotException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    class HiveKey extends BytesWritable {
      private static final int LENGTH_BYTES = 4;

      public int hashCode;
      private boolean hashCodeValid;

      private transient int distKeyLength;

      public HiveKey() {
        hashCodeValid = false;
      }

      public HiveKey(byte[] bytes, int hashcode) {
        super(bytes);
        hashCode = hashcode;
        hashCodeValid = true;
      }

      public void setHashCode(int myHashCode) {
        hashCodeValid = true;
        hashCode = myHashCode;
      }

      @Override
      public int hashCode() {
        if (!hashCodeValid) {
          throw new RuntimeException("Cannot get hashCode() from deserialized "
            + HiveKey.class);
        }
        return hashCode;
      }

      public void setDistKeyLength(int distKeyLength) {
        this.distKeyLength = distKeyLength;
      }

      public int getDistKeyLength() {
        return distKeyLength;
      }
    }

    PicklePot<Tuple2<HiveKey, String>> picklePot = new PicklePotImpl<Tuple2<HiveKey, String>>(baos, null);

    HiveKey hiveKey = new HiveKey("hello".getBytes(), 1);
    picklePot.write(new Tuple2<HiveKey, String>(hiveKey, "intel"));

    picklePot.flush();
    picklePot.close();

    PicklePot<Tuple2<HiveKey, String>> picklepot = new PicklePotImpl(new ByteArrayInputStream(baos.toByteArray()));
    Tuple2<HiveKey, String> result = picklepot.read();

    assertArrayEquals("hello".getBytes(), result._1().getBytes());
    assertEquals(1, result._1().hashCode());
    assertEquals("intel", result._2());
  }
}
