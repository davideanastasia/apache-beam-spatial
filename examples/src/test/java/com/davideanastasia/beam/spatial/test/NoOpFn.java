package com.davideanastasia.beam.spatial.test;

import org.apache.beam.sdk.transforms.DoFn;

public class NoOpFn<T> extends DoFn<T, T> {

    @ProcessElement
    public void processElement(ProcessContext cx) {
        System.out.println(cx.element() + " in " + cx.pane());

        cx.output(cx.element());
    }

}