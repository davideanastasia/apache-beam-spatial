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
