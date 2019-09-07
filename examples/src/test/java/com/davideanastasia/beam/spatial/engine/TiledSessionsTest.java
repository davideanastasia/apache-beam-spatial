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

import com.davideanastasia.beam.spatial.entities.Point;
import com.davideanastasia.beam.spatial.entities.TaxiPoint;
import com.davideanastasia.beam.spatial.utils.TiledSession;
import com.davideanastasia.beam.spatial.utils.TiledSessionCoder;
import com.davideanastasia.beam.spatial.utils.TiledSessionCombineFn;
import com.google.common.base.Preconditions;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TiledSessionsTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Before
    public void before() {
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();

        coderRegistry.registerCoderForClass(TaxiPoint.class, TaxiPoint.TaxiPointCoder.of());
        coderRegistry.registerCoderForClass(Point.class, Point.PointCoder.of());
        coderRegistry.registerCoderForClass(TiledSession.class, TiledSessionCoder.of());
    }

    private TimestampedValue<TaxiPoint> asTimestampedValue(String record) {
        TaxiPoint tp = TaxiPoint.parse(record);
        Preconditions.checkNotNull(tp);
        return TimestampedValue.of(tp, tp.getTs());
    }

    @Test
    public void testTimeOnlyWindow() throws Exception {
        TestStream<TaxiPoint> input = TestStream.create(TaxiPoint.TaxiPointCoder.of())
            .addElements(
                asTimestampedValue("9997,2008-02-08 11:50:12,116.31798,39.84743"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:51:32,116.32452,39.84748"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:51:32,116.32452,39.84748"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:53:23,116.32717,39.84802"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:54:13,116.32715,39.85012"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:55:13,116.3255,39.8502"),    // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:56:14,116.3255,39.8502"),    // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:57:34,116.32712,39.84925"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:58:27,116.32705,39.8481")    // ,8831aa411bfffff
            )
            .advanceWatermarkToInfinity();

        PCollection<KV<Integer, TaxiPoint>> kvInput = pipeline.apply(input)
            .setCoder(TaxiPoint.TaxiPointCoder.of())
            .apply(WithKeys.of(TaxiPoint::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TaxiPoint.TaxiPointCoder.of()));

        PCollection<KV<Integer, TiledSession>> output = kvInput
            .apply(Window.<KV<Integer, TaxiPoint>>into(TiledSessions.<TaxiPoint>withGapDuration(Duration.standardMinutes(5))
                .withStrategy(TiledSessions.SortingStrategy.TIME_ONLY)
                .withResolution(8))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply(Combine.perKey(new TiledSessionCombineFn<>()))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TiledSessionCoder.of()));

        PAssert.that(output)
            .containsInAnyOrder(
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:50:12.000Z"), Instant.parse("2008-02-08T11:51:12.000Z"), "8831aa4119fffff", 4)),
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:51:32.000Z"), Instant.parse("2008-02-08T11:53:23.000Z"), "8831aa411bfffff", 3)),
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:54:13.000Z"), Instant.parse("2008-02-08T11:56:14.000Z"), "8831aa4025fffff", 3)),
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:57:34.000Z"), Instant.parse("2008-02-08T11:58:27.000Z"), "8831aa411bfffff", 2))
            );

        pipeline.run();
    }

    @Test
    public void testSpaceAndTimeWindow() throws Exception {
        TestStream<TaxiPoint> input = TestStream.create(TaxiPoint.TaxiPointCoder.of())
            .addElements(
                asTimestampedValue("9997,2008-02-08 11:50:12,116.31798,39.84743"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa4119fffff
                asTimestampedValue("9997,2008-02-08 11:51:12,116.32157,39.84787"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:51:32,116.32452,39.84748"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:51:32,116.32452,39.84748"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:53:23,116.32717,39.84802"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:54:13,116.32715,39.85012"),  // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:55:13,116.3255,39.8502"),    // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:56:14,116.3255,39.8502"),    // ,8831aa4025fffff
                asTimestampedValue("9997,2008-02-08 11:57:34,116.32712,39.84925"),  // ,8831aa411bfffff
                asTimestampedValue("9997,2008-02-08 11:58:27,116.32705,39.8481")    // ,8831aa411bfffff
            )
            .advanceWatermarkToInfinity();

        PCollection<KV<Integer, TaxiPoint>> kvInput = pipeline.apply(input)
            .setCoder(TaxiPoint.TaxiPointCoder.of())
            .apply(WithKeys.of(TaxiPoint::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TaxiPoint.TaxiPointCoder.of()));

        PCollection<KV<Integer, TiledSession>> output = kvInput
            .apply(Window.<KV<Integer, TaxiPoint>>into(TiledSessions.<TaxiPoint>withGapDuration(Duration.standardMinutes(5))
                .withStrategy(TiledSessions.SortingStrategy.SPACE_AND_TIME)
                .withResolution(8))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply(Combine.perKey(new TiledSessionCombineFn<>()))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TiledSessionCoder.of()));

        PAssert.that(output)
            .containsInAnyOrder(
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:50:12.000Z"), Instant.parse("2008-02-08T11:51:12.000Z"), "8831aa4119fffff", 4)),
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:51:32.000Z"), Instant.parse("2008-02-08T11:58:27.000Z"), "8831aa411bfffff", 5)),
                KV.of(9997, new TiledSession(Instant.parse("2008-02-08T11:54:13.000Z"), Instant.parse("2008-02-08T11:56:14.000Z"), "8831aa4025fffff", 3))
            );

        pipeline.run();
    }


}