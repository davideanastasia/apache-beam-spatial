/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.davideanastasia.beam.spatial.entities;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.FloatCoder;
import org.apache.beam.sdk.coders.StructuredCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Point {

    private final float latitude;
    private final float longitude;

    public Point(float latitude, float longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("latitude", latitude)
            .add("longitude", longitude)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return Float.compare(point.latitude, latitude) == 0 &&
            Float.compare(point.longitude, longitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(latitude, longitude);
    }

    public static class PointCoder extends StructuredCoder<Point> {

        private static final Coder<Point> INSTANCE = new PointCoder();
        private static final Coder<Float> FLOAT_CODER = FloatCoder.of();

        public static Coder<Point> of() {
            return INSTANCE;
        }

        @Override
        public void encode(Point value, OutputStream outStream) throws CoderException, IOException {
            FLOAT_CODER.encode(value.latitude, outStream);
            FLOAT_CODER.encode(value.longitude, outStream);
        }

        @Override
        public Point decode(InputStream inStream) throws CoderException, IOException {
            Float latitude = FLOAT_CODER.decode(inStream);
            Float longitude = FLOAT_CODER.decode(inStream);

            return new Point(latitude, longitude);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            FLOAT_CODER.verifyDeterministic();
        }
    }
}
