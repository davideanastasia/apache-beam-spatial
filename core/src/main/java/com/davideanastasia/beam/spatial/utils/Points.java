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
            return input.apply(Filter.by((SerializableFunction<T, Boolean>) input1 -> input1.getPoint().getLatitude() <= 90.0 && input1.getPoint().getLatitude() >= -90.0));
        }
    }
}
