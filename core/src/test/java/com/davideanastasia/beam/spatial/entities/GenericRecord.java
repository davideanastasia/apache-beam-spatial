package com.davideanastasia.beam.spatial.entities;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.coders.*;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@DefaultCoder(GenericRecord.GenericRecordCoder.class)
public class GenericRecord implements HasPoint, HasTimestamp {

    private final int id;
    private final Instant ts;
    private final Point point;

    public GenericRecord(int id, Instant ts, Point point) {
        this.id = id;
        this.ts = ts;
        this.point = point;
    }

    public int getId() {
        return id;
    }

    public Instant getTs() {
        return ts;
    }

    public Point getPoint() {
        return this.point;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("ts", ts)
            .add("point", point)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenericRecord taxiPoint = (GenericRecord) o;
        return id == taxiPoint.id &&
            ts.isEqual(taxiPoint.ts) &&
            Objects.equals(point, taxiPoint.point);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, point);
    }

    public static class GenericRecordCoder extends StructuredCoder<GenericRecord> {

        private static final Coder<GenericRecord> INSTANCE = new GenericRecordCoder();

        private static final Coder<Integer> INT_CODER = BigEndianIntegerCoder.of();
        private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
        private static final Coder<Point> POINT_CODER = Point.PointCoder.of();

        public static Coder<GenericRecord> of() {
            return INSTANCE;
        }

        @Override
        public void encode(GenericRecord value, OutputStream outStream) throws CoderException, IOException {
            INT_CODER.encode(value.id, outStream);
            INSTANT_CODER.encode(value.ts, outStream);
            POINT_CODER.encode(value.point, outStream);
        }

        @Override
        public GenericRecord decode(InputStream inStream) throws CoderException, IOException {
            Integer id = INT_CODER.decode(inStream);
            Instant ts = INSTANT_CODER.decode(inStream);
            Point point = POINT_CODER.decode(inStream);

            return new GenericRecord(id, ts, point);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            INT_CODER.verifyDeterministic();
            INSTANT_CODER.verifyDeterministic();
            POINT_CODER.verifyDeterministic();
        }
    }
}
