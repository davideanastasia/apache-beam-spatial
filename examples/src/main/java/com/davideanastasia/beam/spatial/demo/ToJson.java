package com.davideanastasia.beam.spatial.demo;

import com.davideanastasia.beam.spatial.utils.TiledSession;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ToJson {

    public static ToJsonTn of() {
        return new ToJsonTn();
    }

    private static class ToJsonTn extends PTransform<PCollection<KV<Integer, TiledSession>>, PCollection<String>> {

        @Override
        public PCollection<String> expand(PCollection<KV<Integer, TiledSession>> input) {
            return input.apply(ParDo.of(new ToJsonFn()));
        }
    }

    private static class ToJsonFn extends DoFn<KV<Integer, TiledSession>, String> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(ProcessContext cx) {
            KV<Integer, TiledSession> element = cx.element();

            String output = String.format("{\"id\":%d, \"startTs\":\"%s\", \"endTs\":\"%s\", \"hexAddr\":\"%s\", \"count\":%d, \"timing\":\"%s\"}",
                element.getKey(),
                element.getValue().getStart().toString(),
                element.getValue().getEnd().toString(),
                element.getValue().getHexAddr(),
                element.getValue().getCount(),
                cx.pane().getTiming().toString());

            cx.output(output);
        }
    }

}
