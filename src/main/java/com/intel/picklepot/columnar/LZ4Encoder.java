package com.intel.picklepot.columnar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

public class LZ4Encoder implements Encoder{
    private OutputStream os;

    @Override
    public OutputStream getOutputStream() {
        if(os == null)
            os = new ByteArrayOutputStream();
        return os;
    }

    @Override
    public void encode(Iterator values) {
        byte[] input = Bytes.toBytes(values);
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        byte[] compressed = new byte[compressor.maxCompressedLength(input.length)];
        int compressedSize = compressor.compress(input, 0, input.length, compressed, 0, compressed.length);
        os = new ByteArrayOutputStream();
        try {
            os.write(compressed, 0, compressedSize);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void encode(Object value) {

    }
}
