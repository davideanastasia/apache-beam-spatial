package com.davideanastasia.beam.spatial.entities;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PointTest {

    @Test
    public void testCompare() {
        Point p1 = new Point(1.000001f, 1.000001f);
        Point p2 = new Point(1.000001f, 1.000001f);

        assertEquals(p1, p2);
    }

}