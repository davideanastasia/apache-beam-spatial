package com.davideanastasia.beam.spatial.demo;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;

public class OnTime {

    public static <T> OnTime.OnTimeTn<T> of() {
        return new OnTime.OnTimeTn<>();
    }

    private static class OnTimeTn<T> extends PTransform<PCollection<T>, PCollection<T>> {

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new OnTimeFn<>()));
        }
    }

    private static class OnTimeFn<T> extends DoFn<T, T> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(ProcessContext cx) {
            if (cx.pane().getTiming() == PaneInfo.Timing.ON_TIME) {
                cx.output(cx.element());
            }
        }

    }

}
