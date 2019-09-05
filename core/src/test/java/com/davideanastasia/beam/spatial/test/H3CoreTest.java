package com.davideanastasia.beam.spatial.test;

import com.uber.h3core.H3Core;
import org.junit.Test;

public class H3CoreTest {

    @Test
    public void testH3Core() throws Exception {
        H3Core h3 = H3Core.newInstance();
        String hexAddr = h3.geoToH3Address(1.f, 1.f, 9);

        System.out.println(hexAddr);
    }

}
