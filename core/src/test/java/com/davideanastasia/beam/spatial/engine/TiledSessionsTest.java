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

import com.davideanastasia.beam.spatial.entities.GenericRecord;
import com.davideanastasia.beam.spatial.entities.Point;
import com.davideanastasia.beam.spatial.test.NoOpFn;
import com.davideanastasia.beam.spatial.utils.TiledSession;
import com.davideanastasia.beam.spatial.utils.TiledSessionCoder;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
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

        coderRegistry.registerCoderForClass(GenericRecord.class, GenericRecord.GenericRecordCoder.of());
        coderRegistry.registerCoderForClass(Point.class, Point.PointCoder.of());
        coderRegistry.registerCoderForClass(TiledSession.class, TiledSessionCoder.of());
    }

    @Test
    public void testBasicGapSplit() throws Exception {
        TestStream<GenericRecord> input = TestStream.create(GenericRecord.GenericRecordCoder.of())
            .addElements(
                TimestampedValue.of(new GenericRecord(1, new Instant(0), new Point(1.f, 1.f)), new Instant(0)),
                TimestampedValue.of(new GenericRecord(1, new Instant(1), new Point(1.000001f, 1.000001f)), new Instant(1)),
                TimestampedValue.of(new GenericRecord(1, new Instant(0).plus(Duration.standardMinutes(2)), new Point(1.000002f, 1.000002f)), new Instant(0).plus(Duration.standardMinutes(2))),
                TimestampedValue.of(new GenericRecord(1, new Instant(1).plus(Duration.standardMinutes(2)), new Point(1.000003f, 1.000003f)), new Instant(1).plus(Duration.standardMinutes(2)))
            )
            .advanceWatermarkToInfinity();

        PCollection<KV<Integer, GenericRecord>> kvInput = pipeline.apply(input)
            .setCoder(GenericRecord.GenericRecordCoder.of())
            .apply(WithKeys.of(GenericRecord::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), GenericRecord.GenericRecordCoder.of()));

        PCollection<KV<Integer, Iterable<GenericRecord>>> output = kvInput
            .apply(Window.<KV<Integer, GenericRecord>>into(TiledSessions.<GenericRecord>withGapDuration(Duration.standardMinutes(1)).withStrategy(TiledSessions.SortingStrategy.TIME_ONLY).withResolution(9))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply(ParDo.of(new NoOpFn<>()));

        TiledIntervalWindow window1 = new TiledIntervalWindow("897541ad5a7ffff", Instant.ofEpochMilli(0), Instant.ofEpochMilli(1).plus(Duration.standardMinutes(1)));
        PAssert
            .that(output)
            .inWindow(window1)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, new Instant(0), new Point(1.f, 1.f)),
                new GenericRecord(1, new Instant(1), new Point(1.000001f, 1.000001f))
            )));

        TiledIntervalWindow window2 = new TiledIntervalWindow("897541ad5a7ffff", Instant.ofEpochMilli(2 * 60 * 1000), Instant.ofEpochMilli(3 * 60 * 1000 + 1));
        PAssert
            .that(output)
            .inWindow(window2)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, new Instant(0).plus(Duration.standardMinutes(2)), new Point(1.000002f, 1.000002f)),
                new GenericRecord(1, new Instant(1).plus(Duration.standardMinutes(2)), new Point(1.000003f, 1.000003f))
            )));

        // run pipeline!
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testSpaceAndTimeSplit() throws Exception {
        TestStream<GenericRecord> input = TestStream.create(GenericRecord.GenericRecordCoder.of())
            .addElements(
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(0), new Point(1.f, 1.f)), Instant.ofEpochSecond(0)),
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(10), new Point(2.000001f, 2.000001f)), Instant.ofEpochSecond(10)),
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(20), new Point(1.000002f, 1.000002f)), Instant.ofEpochSecond(20))
            )
            .advanceWatermarkToInfinity();

        PCollection<KV<Integer, GenericRecord>> kvInput = pipeline.apply(input)
            .setCoder(GenericRecord.GenericRecordCoder.of())
            .apply(WithKeys.of(GenericRecord::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), GenericRecord.GenericRecordCoder.of()));

        PCollection<KV<Integer, Iterable<GenericRecord>>> output = kvInput
            .apply(Window.<KV<Integer, GenericRecord>>into(TiledSessions.<GenericRecord>withGapDuration(Duration.standardSeconds(30)).withStrategy(TiledSessions.SortingStrategy.SPACE_AND_TIME).withResolution(9))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply(ParDo.of(new NoOpFn<>()));

        TiledIntervalWindow window1 = new TiledIntervalWindow("897541ad5a7ffff", Instant.ofEpochSecond(0), Instant.ofEpochSecond(50));
        PAssert
            .that(output)
            .inWindow(window1)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, Instant.ofEpochSecond(0), new Point(1.f, 1.f)),
                new GenericRecord(1, Instant.ofEpochSecond(20), new Point(1.000002f, 1.000002f))
            )));

        TiledIntervalWindow window2 = new TiledIntervalWindow("89756e45da7ffff", Instant.ofEpochSecond(10), Instant.ofEpochSecond(40));
        PAssert
            .that(output)
            .inWindow(window2)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, Instant.ofEpochSecond(10), new Point(2.000001f, 2.000001f))
            )));

        // run pipeline!
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void testTimeOnlySplit() throws Exception {
        TestStream<GenericRecord> input = TestStream.create(GenericRecord.GenericRecordCoder.of())
            .addElements(
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(0), new Point(1.f, 1.f)), Instant.ofEpochSecond(0)),
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(10), new Point(2.000001f, 2.000001f)), Instant.ofEpochSecond(10)),
                TimestampedValue.of(new GenericRecord(1, Instant.ofEpochSecond(20), new Point(1.000002f, 1.000002f)), Instant.ofEpochSecond(20))
            )
            .advanceWatermarkToInfinity();

        PCollection<KV<Integer, GenericRecord>> kvInput = pipeline.apply(input)
            .setCoder(GenericRecord.GenericRecordCoder.of())
            .apply(WithKeys.of(GenericRecord::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), GenericRecord.GenericRecordCoder.of()));

        PCollection<KV<Integer, Iterable<GenericRecord>>> output = kvInput
            .apply(Window.<KV<Integer, GenericRecord>>into(TiledSessions.<GenericRecord>withGapDuration(Duration.standardSeconds(30)).withStrategy(TiledSessions.SortingStrategy.TIME_ONLY).withResolution(9))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply(GroupByKey.create())
            .apply(ParDo.of(new NoOpFn<>()));

        TiledIntervalWindow window1 = new TiledIntervalWindow("897541ad5a7ffff", Instant.ofEpochSecond(0), Instant.ofEpochSecond(30));
        PAssert
            .that(output)
            .inWindow(window1)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, Instant.ofEpochSecond(0), new Point(1.f, 1.f))
            )));

        TiledIntervalWindow window3 = new TiledIntervalWindow("897541ad5a7ffff", Instant.ofEpochSecond(20), Instant.ofEpochSecond(50));
        PAssert
            .that(output)
            .inWindow(window3)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, Instant.ofEpochSecond(20), new Point(1.000002f, 1.000002f))
            )));

        TiledIntervalWindow window2 = new TiledIntervalWindow("89756e45da7ffff", Instant.ofEpochSecond(10), Instant.ofEpochSecond(40));
        PAssert
            .that(output)
            .inWindow(window2)
            .containsInAnyOrder(KV.of(1, Lists.newArrayList(
                new GenericRecord(1, Instant.ofEpochSecond(10), new Point(2.000001f, 2.000001f))
            )));

        // run pipeline!
        pipeline.run().waitUntilFinish();
    }
}