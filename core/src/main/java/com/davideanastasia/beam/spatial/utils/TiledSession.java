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
package com.davideanastasia.beam.spatial.utils;

import com.davideanastasia.beam.spatial.entities.HasPoint;
import com.davideanastasia.beam.spatial.entities.HasTimestamp;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.uber.h3core.H3Core;
import org.joda.time.Instant;

import java.util.Objects;

public class TiledSession {

    private final Instant start;
    private final Instant end;

    private final String hexAddr;
    private final int count;

    public TiledSession(Instant start, Instant end, String hexAddr, int count) {
        this.start = start;
        this.end = end;
        this.hexAddr = hexAddr;
        this.count = count;
    }

    public TiledSession() {
        this.start = null;
        this.end = null;
        this.hexAddr = null;
        this.count = 0;
    }

    public Instant getStart() {
        return start;
    }

    public Instant getEnd() {
        return end;
    }

    public String getHexAddr() {
        return hexAddr;
    }

    public int getCount() {
        return count;
    }

    public <T extends HasPoint & HasTimestamp> TiledSession add(T input) throws Exception {
        H3Core h3 = H3Core.newInstance();

        Instant __start;
        if (start == null) {
            __start = input.getTs();
        } else {
            __start = new Instant(Math.min(start.getMillis(), input.getTs().getMillis()));
        }

        Instant __end;
        if (end == null) {
            __end = input.getTs();
        } else {
            __end = new Instant(Math.max(end.getMillis(), input.getTs().getMillis()));
        }

        String hexAddr = h3.geoToH3Address(
            input.getPoint().getLatitude(),
            input.getPoint().getLongitude(),
            8);

        if (this.hexAddr != null) {
            Preconditions.checkArgument(hexAddr.equals(this.hexAddr));
        }

        return new TiledSession(
            __start,
            __end,
            hexAddr,
            count + 1);
    }

    public TiledSession merge(TiledSession other) {
        Instant __start;
        if (this.start == null || other.start == null) {
            __start = MoreObjects.firstNonNull(this.start, other.start);
        } else {
            __start = new Instant(Math.min(start.getMillis(), other.start.getMillis()));
        }

        Instant __end;
        if (this.end == null || other.end == null) {
            __end = MoreObjects.firstNonNull(this.end, other.end);
        } else {
            __end = new Instant(Math.max(end.getMillis(), other.end.getMillis()));
        }

        if (this.start != null && other.start != null) {
            Preconditions.checkArgument(hexAddr.equals(this.hexAddr));
        }

        return new TiledSession(
            __start,
            __end,
            other.hexAddr,
            count + other.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TiledSession that = (TiledSession) o;
        return count == that.count &&
            start.isEqual(that.start) &&
            end.isEqual(that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, count);
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("start", start)
            .add("end", end)
            .add("hexAddr", hexAddr)
            .add("count", count)
            .toString();
    }
}
