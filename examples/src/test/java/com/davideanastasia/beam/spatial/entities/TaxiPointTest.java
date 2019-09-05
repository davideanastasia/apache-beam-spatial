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