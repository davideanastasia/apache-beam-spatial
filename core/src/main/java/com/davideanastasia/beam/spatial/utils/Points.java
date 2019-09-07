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
import com.google.common.primitives.Floats;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class Points {

    public static <T extends HasPoint> RemoveZero<T> removeZeros() {
        return new Points.RemoveZero<>();
    }

    private static class RemoveZero<T extends HasPoint> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(Filter.by((SerializableFunction<T, Boolean>) input1 -> Floats.compare(input1.getPoint().getLatitude(), 0.0f) != 0 && Floats.compare(input1.getPoint().getLongitude(), 0.0f) != 0));
        }
    }

    public static <T extends HasPoint> RemoveOutOfBoundary<T> removeOutOfBoundary() {
        return new Points.RemoveOutOfBoundary<>();
    }

    private static class RemoveOutOfBoundary<T extends HasPoint> extends PTransform<PCollection<T>, PCollection<T>> {
        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(Filter.by((SerializableFunction<T, Boolean>) input1 ->
                input1.getPoint().getLatitude() >= -90.0 && input1.getPoint().getLatitude() <= 90.0));
        }
    }
}
