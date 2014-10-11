package com.intel.picklepot;

import com.intel.picklepot.columnar.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EncoderTest {
    List list;
    Encoder encoder;
    Decoder decoder;

    @Test
    public void testRunLength() {
        list = Arrays.asList(0, 1, 2);
        encoder = new RunLengthEncoder();
        decoder = new RunLengthDecoder();
        test();
    }

    @Test
    public void testLZ4() {
        list = Arrays.asList("hello", "world");
        encoder = new LZ4Encoder();
        decoder = new LZ4Decoder();
        test();
    }

    public void test() {
        encoder.encode(list.iterator());
        ByteArrayOutputStream bos = (ByteArrayOutputStream) encoder.getOutputStream();

        Iterator iterator = decoder.decode(bos.toByteArray(), String.class.getName());
        for(Object expected : list) {
            assertTrue(iterator.hasNext());
            assertEquals(expected, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }
}