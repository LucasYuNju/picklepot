package com.intel.picklepot.columnar.runlength;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class InStream extends InputStream {
    public static final int END_OF_BUFFER = -1;
    private final boolean useVInts;

    protected InStream(boolean useVInts) {
        this.useVInts = useVInts;
    }

    /**
     * This should only be used if the data happens to already be in memory,
     * e.g. for tests
     */
    public static InStream create(String name, ByteBuffer input,
                                  boolean useVInts) throws IOException {
        return new UncompressedStream(name, input, useVInts);
    }

    public boolean useVInts() {
        return useVInts;
    }

    private static class UncompressedStream extends InStream {
        private final String name;
        // The file this stream is to read data from
        private byte[] array;
        private int offset;
        private final long base;
        private final int limit;

        public UncompressedStream(String name, ByteBuffer input, boolean useVInts) {
            super(useVInts);
            this.name = name;
            this.array = input.array();
            this.base = input.arrayOffset() + input.position();
            this.offset = (int) base;
            this.limit = input.arrayOffset() + input.limit();
        }

        @Override
        public int read() throws IOException {
            return 0xff & array[offset++];
        }

        @Override
        public int available() {
            return limit - offset;
        }

        @Override
        public void close() {
            array = null;
            offset = 0;
        }

        @Override
        public String toString() {
            return "uncompressed stream " + name + " base: " + base +
                    " offset: " + offset + " limit: " + limit;
        }
    }
}