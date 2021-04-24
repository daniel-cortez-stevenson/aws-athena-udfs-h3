/* Licensed under Apache-2.0 2021. */
package io.jyde.aws.athena.connectors.udf.h3;

import static org.junit.Assert.assertEquals;

import com.uber.h3core.H3Core;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class H3AthenaUDFHandlerTest {
    private static final Double lat = 0.;
    private static final Double lng = 0.;
    private static final Integer res = 11;
    private static final String h3address = "8b754e649929fff";
    private static final Integer k = 3;

    private H3AthenaUDFHandler handler;
    private H3Core h3Core;

    @Before
    public void setup() throws IOException {
        this.handler = new H3AthenaUDFHandler();
        this.h3Core = H3Core.newInstance();
    }

    @Test
    public void h3addressisvalid() {
        assertEquals(handler.h3addressisvalid(h3address), h3Core.h3IsValid(h3address));
    }

    @Test
    public void h3addressgetbasecell() {
        assertEquals(
                handler.h3addressgetbasecell(h3address).intValue(),
                h3Core.h3GetBaseCell(h3address));
    }

    @Test
    public void h3addressispentagon() {
        assertEquals(handler.h3addressispentagon(h3address), h3Core.h3IsPentagon(h3address));
    }

    @Test
    public void geotoh3() {
        assertEquals(handler.geotoh3(lat, lng, res).longValue(), h3Core.geoToH3(lat, lng, res));
    }

    @Test
    public void geotoh3address() {
        assertEquals(handler.geotoh3address(lat, lng, res), h3Core.geoToH3Address(lat, lng, res));
    }

    @Test
    public void h3addresskring() {
        assertEquals(handler.h3addresskring(h3address, k), h3Core.kRing(h3address, k));
    }

    @Test
    public void h3addressgetresolution() {
        assertEquals(
                handler.h3addressgetresolution(h3address).intValue(),
                h3Core.h3GetResolution(h3address));
    }

    @Test
    public void h3addresstoparent() {
        assertEquals(
                handler.h3addresstoparent(h3address, res),
                h3Core.h3ToParentAddress(h3address, res));
    }

    @Test
    public void h3addresstochildren() {
        assertEquals(
                handler.h3addresstochildren(h3address, res), h3Core.h3ToChildren(h3address, res));
    }

    @Test
    public void h3addresstocenterchild() {
        assertEquals(
                handler.h3addresstocenterchild(h3address, res),
                h3Core.h3ToCenterChild(h3address, res));
    }

    @Test
    public void h3addressisresclassiii() {
        assertEquals(handler.h3addressisresclassiii(h3address), h3Core.h3IsResClassIII(h3address));
    }

    @Test
    public void stringtoh3() {
        assertEquals(handler.stringtoh3(h3address).longValue(), h3Core.stringToH3(h3address));
    }
}
