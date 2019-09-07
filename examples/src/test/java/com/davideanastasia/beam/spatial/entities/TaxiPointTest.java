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
package com.davideanastasia.beam.spatial.entities;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TaxiPointTest {

    @Test
    public void testParse() {
        String input = "9983,2008-02-02 14:40:41,116.42813,39.90969";

        TaxiPoint taxiPoint = TaxiPoint.parse(input);

        assertNotNull(taxiPoint);

        assertEquals(9983, taxiPoint.getId());
        assertEquals(new DateTime(2008, 2, 2, 14, 40, 41).toInstant(), taxiPoint.getTs());
        assertEquals(new Point(39.90969f, 116.42813f), taxiPoint.getPoint());
    }

}