package com.davideanastasia.beam.spatial.engine;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class TiledIntervalWindow extends BoundedWindow {

    private final String hexAddr;
    private final Instant start;
    private final Instant end;

    public TiledIntervalWindow(String hexAddr, Instant start, Instant end) {
        this.hexAddr = hexAddr;
        this.start = start;
        this.end = end;
    }

    public TiledIntervalWindow(String hexAddr, Instant start, ReadableDuration size) {
        this.hexAddr = hexAddr;
        this.start = start;
        this.end = start.plus(size);
    }

    public String getHexAddr() {
        return hexAddr;
    }

    public Instant start() {
        return start;
    }

    public Instant end() {
        return end;
    }

    public Instant maxTimestamp() {
        return end.minus(1L);
    }

//    public boolean contains(TiledIntervalWindow other) {
//        return this.hexAddr.equals(other.hexAddr) && !this.start.isAfter(other.start) && !this.end.isBefore(other.end);
//    }

    private boolean isDisjoint(TiledIntervalWindow other) {
        return !this.end.isAfter(other.start) || !other.end.isAfter(this.start);
    }

    public boolean intersects(TiledIntervalWindow other) {
        return this.hexAddr.equals(other.hexAddr) && !this.isDisjoint(other);
    }

    public TiledIntervalWindow span(TiledIntervalWindow other) {
        Preconditions.checkArgument(other.hexAddr.equals(hexAddr));

        return new TiledIntervalWindow(
            this.hexAddr,
            new Instant(Math.min(this.start.getMillis(), other.start.getMillis())),
            new Instant(Math.max(this.end.getMillis(), other.end.getMillis()))
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TiledIntervalWindow that = (TiledIntervalWindow) o;
        return Objects.equals(hexAddr, that.hexAddr) &&
            start.isEqual(that.start) &&
            end.isEqual(that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hexAddr, start, end);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("start", start)
            .add("end", end)
            .add("hexAddr", hexAddr)
            .toString();
    }

    public static Coder<TiledIntervalWindow> getCoder() {
        return TiledIntervalWindowCoder.of();
    }

    public static final Comparator<TiledIntervalWindow> TIME_ONLY_COMPARATOR = (o1, o2) -> ComparisonChain.start()
        .compare(o1.start, o2.start)
        .compare(o1.end, o2.end)
        .result();

    public static final Comparator<TiledIntervalWindow> SPACE_AND_TIME_COMPARATOR = (o1, o2) -> ComparisonChain.start()
        .compare(o1.hexAddr, o2.hexAddr)
        .compare(o1.start, o2.start)
        .compare(o1.end, o2.end)
        .result();

    public static class TiledIntervalWindowCoder extends StructuredCoder<TiledIntervalWindow> {
        private static final TiledIntervalWindowCoder INSTANCE = new TiledIntervalWindowCoder();

        private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
        private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
        private static final Coder<ReadableDuration> DURATION_CODER = DurationCoder.of();

        public TiledIntervalWindowCoder() {
        }

        public static TiledIntervalWindowCoder of() {
            return INSTANCE;
        }

        public void encode(TiledIntervalWindow window, OutputStream outStream) throws IOException, CoderException {
            STRING_CODER.encode(window.hexAddr, outStream);
            INSTANT_CODER.encode(window.end, outStream);
            DURATION_CODER.encode(new Duration(window.start, window.end), outStream);
        }

        public TiledIntervalWindow decode(InputStream inStream) throws IOException, CoderException {
            String hexAddr = STRING_CODER.decode(inStream);
            Instant end = INSTANT_CODER.decode(inStream);
            ReadableDuration duration = DURATION_CODER.decode(inStream);
            return new TiledIntervalWindow(hexAddr, end.minus(duration), end);
        }

        public void verifyDeterministic() throws NonDeterministicException {
            INSTANT_CODER.verifyDeterministic();
            DURATION_CODER.verifyDeterministic();
        }

        public boolean consistentWithEquals() {
            return INSTANT_CODER.consistentWithEquals() && DURATION_CODER.consistentWithEquals();
        }

        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }
    }
}
