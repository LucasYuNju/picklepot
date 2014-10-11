package com.intel.picklepot.columnar;

import com.intel.picklepot.columnar.runlength.InStream;
import com.intel.picklepot.columnar.runlength.RunLengthIntegerReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class RunLengthDecoder implements Decoder{
    @Override
    public Iterator decode(byte[] bytes, String className) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
        byteBuffer.put(bytes);
        byteBuffer.flip();
        RunLengthIntegerReader reader = null;
        try {
            reader = new RunLengthIntegerReader(InStream.create("test", byteBuffer, true), true, Integer.SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Integer> list = new LinkedList<Integer>();
        try {
            while(reader.hasNext()) {
                list.add((int) reader.next());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list.iterator();
    }
}
