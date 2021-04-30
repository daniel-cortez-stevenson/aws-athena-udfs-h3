/* Licensed under Apache-2.0 2021. */
package io.jyde.aws.athena.connectors.udf.h3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;
import com.uber.h3core.exceptions.DistanceUndefinedException;
import com.uber.h3core.exceptions.LineUndefinedException;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class H3AthenaUDFHandlerTest {
    private static final Double lat = 0.;
    private static final Double lng = 0.;
    private static final Integer res = 11;
    private static final Long h3 = 628064021095030783L;
    private static final Long secondH3 = 628064007838769151L;
    private static final String h3address = "8b754e649929fff";
    private static final String secondH3Address = "8b754e333701fff";
    private static final Integer k = 3;
    private static final String unit = "m2";

    private H3AthenaUDFHandler handler;
    private H3Core h3Core;

    @Before
    public void setup() throws IOException {
        this.handler = new H3AthenaUDFHandler();
        this.h3Core = H3Core.newInstance();
    }

    @Test
    public void h3isvalid() {
        assertEquals(handler.h3isvalid(h3), h3Core.h3IsValid(h3));
    }

    @Test
    public void h3addressisvalid() {
        assertEquals(handler.h3addressisvalid(h3address), h3Core.h3IsValid(h3address));
    }

    @Test
    public void h3getbasecell() {
        assertEquals(handler.h3getbasecell(h3).intValue(), h3Core.h3GetBaseCell(h3));
    }

    @Test
    public void h3addressgetbasecell() {
        assertEquals(
                handler.h3addressgetbasecell(h3address).intValue(),
                h3Core.h3GetBaseCell(h3address));
    }

    @Test
    public void h3ispentagon() {
        assertEquals(handler.h3ispentagon(h3), h3Core.h3IsPentagon(h3));
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
    public void h3kring() {
        assertEquals(handler.h3kring(h3, k), h3Core.kRing(h3, k));
    }

    @Test
    public void h3addresskring() {
        assertEquals(handler.h3addresskring(h3address, k), h3Core.kRing(h3address, k));
    }

    @Test
    public void h3distance() throws DistanceUndefinedException {
        assertEquals(handler.h3distance(h3, secondH3).intValue(), h3Core.h3Distance(h3, secondH3));
    }

    @Test
    public void h3addressdistance() throws DistanceUndefinedException {
        assertEquals(
                handler.h3addressdistance(h3address, secondH3Address).intValue(),
                h3Core.h3Distance(h3address, secondH3Address));
    }

    @Test
    public void h3line() throws DistanceUndefinedException, LineUndefinedException {
        assertArrayEquals(
                handler.h3line(h3, secondH3).toArray(), h3Core.h3Line(h3, secondH3).toArray());
    }

    @Test
    public void h3addressline() throws DistanceUndefinedException, LineUndefinedException {
        assertArrayEquals(
                handler.h3addressline(h3address, secondH3Address).toArray(),
                h3Core.h3Line(h3address, secondH3Address).toArray());
    }

    @Test
    public void h3getresolution() {
        assertEquals(handler.h3getresolution(h3).intValue(), h3Core.h3GetResolution(h3));
    }

    @Test
    public void h3addressgetresolution() {
        assertEquals(
                handler.h3addressgetresolution(h3address).intValue(),
                h3Core.h3GetResolution(h3address));
    }

    @Test
    public void h3toparent() {
        assertEquals(handler.h3toparent(h3, res).longValue(), h3Core.h3ToParent(h3, res));
    }

    @Test
    public void h3addresstoparent() {
        assertEquals(
                handler.h3addresstoparent(h3address, res),
                h3Core.h3ToParentAddress(h3address, res));
    }

    @Test
    public void h3tochildren() {
        assertArrayEquals(
                handler.h3tochildren(h3, res).toArray(), h3Core.h3ToChildren(h3, res).toArray());
    }

    @Test
    public void h3addresstochildren() {
        assertEquals(
                handler.h3addresstochildren(h3address, res), h3Core.h3ToChildren(h3address, res));
    }

    @Test
    public void h3tocenterchild() {
        assertEquals(handler.h3tocenterchild(h3, res).longValue(), h3Core.h3ToCenterChild(h3, res));
    }

    @Test
    public void h3addresstocenterchild() {
        assertEquals(
                handler.h3addresstocenterchild(h3address, res),
                h3Core.h3ToCenterChild(h3address, res));
    }

    @Test
    public void h3isresclassiii() {
        assertEquals(handler.h3isresclassiii(h3), h3Core.h3IsResClassIII(h3));
    }

    @Test
    public void h3addressisresclassiii() {
        assertEquals(handler.h3addressisresclassiii(h3address), h3Core.h3IsResClassIII(h3address));
    }

    @Test
    public void h3tostring() {
        assertEquals(handler.h3tostring(h3), h3Core.h3ToString(h3));
    }

    @Test
    public void stringtoh3() {
        assertEquals(handler.stringtoh3(h3address).longValue(), h3Core.stringToH3(h3address));
    }

    @Test
    public void h3area() {
        assertTrue(
                almostEqual(
                        handler.h3area(h3, unit).doubleValue(),
                        h3Core.cellArea(h3, AreaUnit.valueOf(unit)),
                        1e-8));
    }

    @Test
    public void h3addressarea() {
        assertTrue(
                almostEqual(
                        handler.h3addressarea(h3address, unit).doubleValue(),
                        h3Core.cellArea(h3address, AreaUnit.valueOf(unit)),
                        1e-8));
    }

    private static boolean almostEqual(double a, double b, double eps) {
        return Math.abs(a - b) < eps;
    }
}
