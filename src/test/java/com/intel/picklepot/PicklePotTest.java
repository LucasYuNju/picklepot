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
import org.junit.Test;

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
}
