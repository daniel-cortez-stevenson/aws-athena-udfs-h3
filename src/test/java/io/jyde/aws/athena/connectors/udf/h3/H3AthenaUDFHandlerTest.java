/* Licensed under Apache-2.0 2021. */
package io.jyde.aws.athena.connectors.udf.h3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class H3AthenaUDFHandlerTest {
    private static final Double lat = 0.;
    private static final Double lng = 0.;
    private static final Integer res = 11;
    private static final String h3address = "8b754e649929fff";

    private H3AthenaUDFHandler handler;

    @Before
    public void setup() throws IOException {
        this.handler = new H3AthenaUDFHandler();
    }

    @Test
    public void geotoh3addressSucceeds() {
        assertEquals(handler.geotoh3address(this.lat, this.lng, this.res), this.h3address);
    }

    @Test
    public void h3addressisvalidForRealH3() {
        assertTrue(handler.h3addressisvalid(this.h3address));
    }
}
