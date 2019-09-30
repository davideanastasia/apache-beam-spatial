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
package com.davideanastasia.beam.spatial.bigquery;

import com.davideanastasia.beam.spatial.utils.TiledSession;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.values.KV;

import java.util.List;

public class TiledSessionTableSpec {

    public static final String FLD_TAXI_ID = "taxi_id";
    public static final String FLD_START_TS = "start_ts";
    public static final String FLD_END_TS = "end_ts";
    public static final String FLD_HEX_ADDR = "hex_addr";
    public static final String FLD_COUNT = "count";

    public static final List<TableFieldSchema> SCHEMA = ImmutableList.of(
        new TableFieldSchema().setName(FLD_TAXI_ID).setType("INTEGER").setMode("REQUIRED"),
        new TableFieldSchema().setName(FLD_START_TS).setType("TIMESTAMP").setMode("REQUIRED"),
        new TableFieldSchema().setName(FLD_END_TS).setType("TIMESTAMP").setMode("REQUIRED"),
        new TableFieldSchema().setName(FLD_HEX_ADDR).setType("STRING"),
        new TableFieldSchema().setName(FLD_COUNT).setType("INTEGER")
    );

    public static TableRow toTableRow(KV<Integer, TiledSession> tiledSession) {
        return new TableRow()
            .set(TiledSessionTableSpec.FLD_TAXI_ID, tiledSession.getKey())
            .set(TiledSessionTableSpec.FLD_START_TS, ((double)tiledSession.getValue().getStart().getMillis() / 1000.0))
            .set(TiledSessionTableSpec.FLD_END_TS, ((double)tiledSession.getValue().getEnd().getMillis() / 1000.0))
            .set(TiledSessionTableSpec.FLD_HEX_ADDR, tiledSession.getValue().getHexAddr())
            .set(TiledSessionTableSpec.FLD_COUNT, tiledSession.getValue().getCount());
    }

}
