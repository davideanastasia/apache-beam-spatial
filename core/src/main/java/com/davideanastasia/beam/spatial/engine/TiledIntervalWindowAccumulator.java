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
package com.davideanastasia.beam.spatial.engine;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.windowing.WindowFn;

import java.util.ArrayList;
import java.util.List;

public class TiledIntervalWindowAccumulator {

    private TiledIntervalWindow union;
    private final List<TiledIntervalWindow> parts;

    public TiledIntervalWindowAccumulator() {
        this.union = null;
        this.parts = new ArrayList<>();
    }

    public TiledIntervalWindowAccumulator(TiledIntervalWindow window) {
        this.union = window;
        this.parts = Lists.newArrayList(window);
    }

    public boolean intersects(TiledIntervalWindow window) {
        return this.union == null || this.union.intersects(window);
    }

    public void add(TiledIntervalWindow window) {
        this.union = this.union == null ? window : this.union.span(window);
        this.parts.add(window);
    }

    public void apply(WindowFn<?, TiledIntervalWindow>.MergeContext c) throws Exception {
        if (this.parts.size() > 1) {
            c.merge(this.parts, this.union);
        }
    }

    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("union", union)
            .add("parts", parts)
            .toString();
    }

}
