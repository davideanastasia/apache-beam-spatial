package com.davideanastasia.beam.spatial.utils;

import com.davideanastasia.beam.spatial.entities.TaxiPoint;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class TaxiPoints {

    public static Parse parse() {
        return new TaxiPoints.Parse();
    }

    private static class Parse extends PTransform<PCollection<String>, PCollection<TaxiPoint>> {

        @Override
        public PCollection<TaxiPoint> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new StringToTaxiPoint()));
        }
    }

    private static class StringToTaxiPoint extends DoFn<String, TaxiPoint> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(ProcessContext cx) {
            String data = cx.element();
            TaxiPoint taxiPoint = TaxiPoint.parse(data);

            if (taxiPoint != null) {
                cx.output(taxiPoint);
            }
        }

    }

}



