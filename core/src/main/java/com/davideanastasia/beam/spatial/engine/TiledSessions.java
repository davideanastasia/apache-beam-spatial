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
package com.davideanastasia.beam.spatial.engine;

import avro.shaded.com.google.common.collect.Lists;
import com.davideanastasia.beam.spatial.entities.HasPoint;
import com.davideanastasia.beam.spatial.entities.Point;
import com.uber.h3core.H3Core;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

public class TiledSessions<T extends HasPoint> extends WindowFn<KV<?, T>, TiledIntervalWindow> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiledSessions.class);
    private static final int H3_RES_DEFAULT = 8;

    public enum SortingStrategy {
        TIME_ONLY,
        SPACE_AND_TIME
    }

    private final Duration gapDuration;
    private int h3Resolution = H3_RES_DEFAULT;
    private SortingStrategy sortingStrategy = SortingStrategy.SPACE_AND_TIME;

    public static <TS extends HasPoint> TiledSessions<TS> withGapDuration(Duration gapDuration) {
        return new TiledSessions<>(gapDuration);
    }

    private TiledSessions(Duration gapDuration) {
        this.gapDuration = gapDuration;
    }

    public TiledSessions<T> withResolution(int resolution) {
        this.h3Resolution = resolution;
        return this;
    }

    public TiledSessions<T> withStrategy(SortingStrategy sortingStrategy) {
        this.sortingStrategy = sortingStrategy;
        return this;
    }

    @Override
    public Collection<TiledIntervalWindow> assignWindows(AssignContext c) throws Exception {
        H3Core h3 = H3Core.newInstance();

        Point point = c.element().getValue().getPoint();
        String hexAddr = h3.geoToH3Address(point.getLatitude(), point.getLongitude(), h3Resolution);

        TiledIntervalWindow tiw = new TiledIntervalWindow(
            hexAddr,
            c.timestamp(),
            gapDuration);

//        LOGGER.info("Assign /0 {}", tiw);
        return Lists.newArrayList(tiw);
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
        List<TiledIntervalWindow> sortedWindows = Lists.newArrayList(c.windows());
        switch (sortingStrategy) {
            case SPACE_AND_TIME:
                sortedWindows.sort(TiledIntervalWindow.SPACE_AND_TIME_COMPARATOR);
                break;
            case TIME_ONLY:
                sortedWindows.sort(TiledIntervalWindow.TIME_ONLY_COMPARATOR);
                break;
        }

        List<TiledIntervalWindowAccumulator> merges = Lists.newArrayList();
        TiledIntervalWindowAccumulator accumulator = new TiledIntervalWindowAccumulator();
        for (TiledIntervalWindow currentWindow : sortedWindows) {
            if (accumulator.intersects(currentWindow)) {
                accumulator.add(currentWindow);
            } else {
                merges.add(accumulator);
//                LOGGER.info("Completed merge /1 {}", accumulator);

                accumulator = new TiledIntervalWindowAccumulator(currentWindow);
            }
        }
        merges.add(accumulator);
//        LOGGER.info("Completed merge /2 {}", accumulator);

        for (TiledIntervalWindowAccumulator merge : merges) {
            merge.apply(c);
        }

//        LOGGER.info("Done");
    }

    @SuppressWarnings("deprecated")
    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return false;
    }

    @Override
    public Coder<TiledIntervalWindow> windowCoder() {
        return TiledIntervalWindow.TiledIntervalWindowCoder.of();
    }

    @Override
    public WindowMappingFn<TiledIntervalWindow> getDefaultWindowMappingFn() {
        throw new UnsupportedOperationException("Sessions is not allowed in side inputs");
    }
}
