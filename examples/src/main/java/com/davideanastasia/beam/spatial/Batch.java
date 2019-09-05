package com.davideanastasia.beam.spatial;

import com.davideanastasia.beam.spatial.engine.TiledSessions;
import com.davideanastasia.beam.spatial.entities.Point;
import com.davideanastasia.beam.spatial.entities.TaxiPoint;
import com.davideanastasia.beam.spatial.utils.*;
import com.google.common.base.Joiner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class Batch {

    public interface Options extends GcpOptions {
        @Description("Path of input files")
        @Default.String("data/input/9997.txt")
        String getInputFile();

        @SuppressWarnings("unused")
        void setInputFile(String value);

        @Description("path of output files")
        @Default.String("data/sessions/output")
        String getOutputFile();

        @SuppressWarnings("unused")
        void setOutputFile(String value);
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
            .apply("Input", TextIO.read().from(options.getInputFile()))
            .apply("StringToTaxiPoint", TaxiPoints.parse())
            .apply("Points", Points.removeZeros())
            .apply("WithTimestamps", WithTimestamps.of(TaxiPoint::getTs))
            .apply("ToKV", WithKeys.of(TaxiPoint::getId))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TaxiPoint.TaxiPointCoder.of()))
            .apply("TiledSessions", Window.<KV<Integer, TaxiPoint>>into(TiledSessions.<TaxiPoint>withGapDuration(Duration.standardMinutes(6)).withStrategy(TiledSessions.SortingStrategy.SPACE_AND_TIME))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
            .apply("TiledSessionCombine", Combine.perKey(new TiledSessionCombineFn<>()))
            .setCoder(KvCoder.of(BigEndianIntegerCoder.of(), TiledSessionCoder.of()));

        taxiPoints
            .apply("MapToString", MapElements.via(new SimpleFunction<KV<Integer, TiledSession>, String>() {
                @Override
                public String apply(KV<Integer, TiledSession> item) {
                    return Joiner.on(",").join(
                        item.getKey(),
                        item.getValue().getStart(),
                        item.getValue().getEnd(),
                        item.getValue().getHexAddr(),
                        item.getValue().getCount());
                }
            }))
            .apply("Output", TextIO.write().to(options.getOutputFile()));

        pipeline.run();
    }

}
