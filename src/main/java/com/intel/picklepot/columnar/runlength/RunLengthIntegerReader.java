package com.intel.picklepot.columnar.runlength;

import java.io.EOFException;
import java.io.IOException;

/**
 * A reader that reads a sequence of integers.
 * */
public class RunLengthIntegerReader {
    private final InStream input;
    private final boolean signed;
    private final int numBytes;
    private final boolean useVInts;
    private final long[] literals = new long[RunLengthConstants.MAX_LITERAL_SIZE];
    private int numLiterals = 0;
    private int delta = 0;
    private int used = 0;
    private boolean repeat = false;

    public RunLengthIntegerReader(InStream input, boolean signed, int numBytes)
            throws IOException {
        this.input = input;
        this.signed = signed;
        this.numBytes = numBytes;
        this.useVInts = input.useVInts();
    }

    private void readValues() throws IOException {
        int control = input.read();
        if (control == -1) {
            throw new EOFException("Read past end of RLE integer from " + input);
        } else if (control < 0x80) {
            numLiterals = control + RunLengthConstants.MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new EOFException("End of stream in RLE Integer from " + input);
            }
            // convert from 0 to 255 to -128 to 127 by converting to a signed
            // byte
            delta = (byte) (0 + delta);
            literals[0] = SerializationUtils.readIntegerType(input, numBytes,
                    signed, useVInts);
        } else {
            repeat = false;
            numLiterals = 0x100 - control;
            used = 0;
            for (int i = 0; i < numLiterals; ++i) {
                literals[i] = SerializationUtils.readIntegerType(input,
                        numBytes, signed, useVInts);
            }
        }
    }

    boolean hasNext() throws IOException {
        return used != numLiterals || input.available() > 0;
    }

    public long next() throws IOException {
        long result;
        if (used == numLiterals) {
            readValues();
        }
        if (repeat) {
            result = literals[0] + (used++) * delta;
        } else {
            result = literals[used++];
        }
        return result;
    }

    public void close() throws IOException {
        input.close();
    }
}