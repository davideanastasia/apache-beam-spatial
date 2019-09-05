package com.davideanastasia.beam.spatial.utils;

import com.davideanastasia.beam.spatial.entities.HasPoint;
import com.davideanastasia.beam.spatial.entities.HasTimestamp;
import org.apache.beam.sdk.transforms.Combine;

public class TiledSessionCombineFn<T extends HasPoint & HasTimestamp> extends Combine.CombineFn<T, TiledSession, TiledSession> {
    @Override
    public TiledSession createAccumulator() {
        return new TiledSession();
    }

    @Override
    public TiledSession addInput(TiledSession mutableAccumulator, T input) {
        try {
            return mutableAccumulator.add(input);
        } catch (Exception e) {
            e.printStackTrace();
            return createAccumulator();
        }
    }

    @Override
    public TiledSession mergeAccumulators(Iterable<TiledSession> accumulators) {
        TiledSession finalAccumulator = createAccumulator();
        for (TiledSession currentAccumulator: accumulators) {
            finalAccumulator = finalAccumulator.merge(currentAccumulator);
        }
        return finalAccumulator;
    }

    @Override
    public TiledSession extractOutput(TiledSession accumulator) {
        return accumulator;
    }
}
