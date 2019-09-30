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
package com.davideanastasia.beam.spatial;

import com.davideanastasia.beam.spatial.bigquery.TiledSessionTableSpec;
import com.davideanastasia.beam.spatial.engine.TiledSessions;
import com.davideanastasia.beam.spatial.entities.Point;
import com.davideanastasia.beam.spatial.entities.TaxiPoint;
import com.davideanastasia.beam.spatial.utils.*;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class Stream {

    private static final String TIMESTAMP_ATTRIBUTE_KEY = "ts_attr_key";

    public interface Options extends GcpOptions {
        @Description("Pubsub Topic Name")
        String getTopicName();

        @SuppressWarnings("unused")
        void setTopicName(String value);
    }

    public static void main(String[] args) {

        Options options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(Options.class);
        Pipeline pipeline = Pipeline.create(options);
        CoderRegistry coderRegistry = pipeline.getCoderRegistry();

        coderRegistry.registerCoderForClass(TaxiPoint.class, TaxiPoint.TaxiPointCoder.of());
        coderRegistry.registerCoderForClass(Point.class, Point.PointCoder.of());
        coderRegistry.registerCoderForClass(TiledSession.class, TiledSessionCoder.of());

        PCollection<KV<Integer, TiledSession>> taxiPoints = pipeline
            .apply("Input", PubsubIO.readStrings()
                .withTimestampAttribute(TIMESTAMP_ATTRIBUTE_KEY)
                .fromTopic(options.getTopicName()))
            .apply("StringToTaxiPoint", TaxiPoints.parse())
            .apply("RemoveZeros", Points.removeZeros())
            .apply("RemoveOutOfBoundaries", Points.removeOutOfBoundary())
            .apply("ToKV", WithKeys.of(TaxiPoint::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TaxiPoint.TaxiPointCoder.of()))
            .apply("TiledSessions", Window.<KV<Integer, TaxiPoint>>into(TiledSessions.<TaxiPoint>withGapDuration(Duration.standardMinutes(6)).withStrategy(TiledSessions.SortingStrategy.SPACE_AND_TIME))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply("TiledSessionCombine", Combine.perKey(new TiledSessionCombineFn<>()))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TiledSessionCoder.of()));

        taxiPoints.apply("Output", BigQueryIO.<KV<Integer, TiledSession>>write()
            .to(new TableReference()
                .setProjectId(options.getProject())
                .setDatasetId("t_drive_data")
                .setTableId("session_data"))
            .withFormatFunction(TiledSessionTableSpec::toTableRow)
            .withSchema(new TableSchema().setFields(TiledSessionTableSpec.SCHEMA))
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }

}
