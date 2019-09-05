package com.davideanastasia.beam.spatial.utils;

import org.apache.beam.sdk.coders.*;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

public class TiledSessionCoder extends StructuredCoder<TiledSession> {

    private static final Coder<TiledSession> INSTANCE = new TiledSessionCoder();

    private static final Coder<Integer> INT_CODER = BigEndianIntegerCoder.of();
    private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

    public static Coder<TiledSession> of() {
        return INSTANCE;
    }

    @Override
    public void encode(TiledSession value, OutputStream outStream) throws CoderException, IOException {
        INSTANT_CODER.encode(value.getStart(), outStream);
        INSTANT_CODER.encode(value.getEnd(), outStream);
        STRING_CODER.encode(value.getHexAddr(), outStream);
        INT_CODER.encode(value.getCount(), outStream);
    }

    @Override
    public TiledSession decode(InputStream inStream) throws CoderException, IOException {
        Instant start = INSTANT_CODER.decode(inStream);
        Instant end = INSTANT_CODER.decode(inStream);
        String hexAddr = STRING_CODER.decode(inStream);
        Integer count = INT_CODER.decode(inStream);

        return new TiledSession(start, end, hexAddr, count);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        INSTANT_CODER.verifyDeterministic();
        INT_CODER.verifyDeterministic();
    }
}