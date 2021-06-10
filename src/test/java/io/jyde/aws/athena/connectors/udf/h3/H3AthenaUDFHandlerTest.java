/* Licensed under Apache-2.0 2021. */
package io.jyde.aws.athena.connectors.udf.h3;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
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

    private Long nearbyH3WithDifferentResolution;
    private String nearbyH3AddressWithDifferentResolution;
    private Long tooFarAwayH3;
    private String tooFarAwayH3Address;

    @Before
    public void setup() throws IOException {
        this.handler = new H3AthenaUDFHandler();
        this.h3Core = H3Core.newInstance();

        this.nearbyH3WithDifferentResolution = h3Core.geoToH3(lat + 0.0001, lng + 0.0001, res - 1);
        this.nearbyH3AddressWithDifferentResolution =
                h3Core.geoToH3Address(lat + 0.0001, lng + 0.0001, res - 1);
        this.tooFarAwayH3 = h3Core.geoToH3(lat + 45., lng + 45., res);
        this.tooFarAwayH3Address = h3Core.geoToH3Address(lat + 45., lng + 45., res);
    }

    @Test
    public void h3_is_valid() {
        assertEquals(handler.h3_is_valid(h3), h3Core.h3IsValid(h3));
    }

    @Test
    public void h3_address_is_valid() {
        assertEquals(handler.h3_is_valid(h3address), h3Core.h3IsValid(h3address));
    }

    @Test
    public void h3_get_base_cell() {
        assertEquals(handler.h3_get_base_cell(h3).intValue(), h3Core.h3GetBaseCell(h3));
    }

    @Test
    public void h3_address_get_base_cell() {
        assertEquals(
                handler.h3_get_base_cell(h3address).intValue(), h3Core.h3GetBaseCell(h3address));
    }

    @Test
    public void h3_is_pentagon() {
        assertEquals(handler.h3_is_pentagon(h3), h3Core.h3IsPentagon(h3));
    }

    @Test
    public void h3_address_is_pentagon() {
        assertEquals(handler.h3_is_pentagon(h3address), h3Core.h3IsPentagon(h3address));
    }

    @Test
    public void geo_to_h3() {
        assertEquals(handler.geo_to_h3(lat, lng, res).longValue(), h3Core.geoToH3(lat, lng, res));
    }

    @Test
    public void geo_to_h3_address() {
        assertEquals(
                handler.geo_to_h3_address(lat, lng, res), h3Core.geoToH3Address(lat, lng, res));
    }

    @Test
    public void k_ring() {
        assertEquals(handler.k_ring(h3, k), h3Core.kRing(h3, k));
    }

    @Test
    public void address_k_ring() {
        assertEquals(handler.k_ring(h3address, k), h3Core.kRing(h3address, k));
    }

    @Test
    public void h3_distance() throws DistanceUndefinedException {
        assertEquals(handler.h3_distance(h3, secondH3).intValue(), h3Core.h3Distance(h3, secondH3));
    }

    @Test
    public void h3_distance_WithNullInputReturnsNull() {
        assertNull(handler.h3_distance(h3, null));
    }

    @Test
    public void h3_distance_TooFarApartReturnsNegativeOne() {
        assertEquals(Integer.valueOf(-1), handler.h3_distance(h3, tooFarAwayH3));
    }

    @Test
    public void h3_distance_ResolutionMismatchThrowsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> handler.h3_distance(h3, nearbyH3WithDifferentResolution));
    }

    @Test
    public void h3_address_distance() throws DistanceUndefinedException {
        assertEquals(
                handler.h3_distance(h3address, secondH3Address).intValue(),
                h3Core.h3Distance(h3address, secondH3Address));
    }

    @Test
    public void h3_address_distance_WithNullReturnsNull() {
        assertNull(handler.h3_distance(h3address, null));
    }

    @Test
    public void h3_address_distance_TooFarApartReturnsNegativeOne() {
        assertEquals(Integer.valueOf(-1), handler.h3_distance(h3address, tooFarAwayH3Address));
    }

    @Test
    public void h3_address_distance_ResolutionMismatchThrowsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> handler.h3_distance(h3address, nearbyH3AddressWithDifferentResolution));
    }

    @Test
    public void h3_line() throws DistanceUndefinedException, LineUndefinedException {
        assertArrayEquals(
                handler.h3_line(h3, secondH3).toArray(), h3Core.h3Line(h3, secondH3).toArray());
    }

    @Test
    public void h3_address_line() throws DistanceUndefinedException, LineUndefinedException {
        assertArrayEquals(
                handler.h3_line(h3address, secondH3Address).toArray(),
                h3Core.h3Line(h3address, secondH3Address).toArray());
    }

    @Test
    public void h3_get_resolution() {
        assertEquals(handler.h3_get_resolution(h3).intValue(), h3Core.h3GetResolution(h3));
    }

    @Test
    public void h3_address_get_resolution() {
        assertEquals(
                handler.h3_get_resolution(h3address).intValue(), h3Core.h3GetResolution(h3address));
    }

    @Test
    public void h3_to_parent() {
        assertEquals(handler.h3_to_parent(h3, res).longValue(), h3Core.h3ToParent(h3, res));
    }

    @Test
    public void h3_address_to_parent() {
        assertEquals(
                handler.h3_to_parent(h3address, res), h3Core.h3ToParentAddress(h3address, res));
    }

    @Test
    public void h3_to_children() {
        assertArrayEquals(
                handler.h3_to_children(h3, res).toArray(), h3Core.h3ToChildren(h3, res).toArray());
    }

    @Test
    public void h3_address_to_children() {
        assertEquals(handler.h3_to_children(h3address, res), h3Core.h3ToChildren(h3address, res));
    }

    @Test
    public void h3_to_center_child() {
        assertEquals(
                handler.h3_to_center_child(h3, res).longValue(), h3Core.h3ToCenterChild(h3, res));
    }

    @Test
    public void h3_address_to_center_child() {
        assertEquals(
                handler.h3_to_center_child(h3address, res), h3Core.h3ToCenterChild(h3address, res));
    }

    @Test
    public void h3_is_res_class_iii() {
        assertEquals(handler.h3_is_res_class_iii(h3), h3Core.h3IsResClassIII(h3));
    }

    @Test
    public void h3_address_is_res_class_iii() {
        assertEquals(handler.h3_is_res_class_iii(h3address), h3Core.h3IsResClassIII(h3address));
    }

    @Test
    public void h3_to_string() {
        assertEquals(handler.h3_to_string(h3), h3Core.h3ToString(h3));
    }

    @Test
    public void string_to_h3() {
        assertEquals(handler.string_to_h3(h3address).longValue(), h3Core.stringToH3(h3address));
    }

    @Test
    public void h3_area() {
        assertTrue(
                almostEqual(
                        handler.h3_area(h3, unit).doubleValue(),
                        h3Core.cellArea(h3, AreaUnit.valueOf(unit)),
                        1e-8));
    }

    @Test
    public void h3_address_area() {
        assertTrue(
                almostEqual(
                        handler.h3_area(h3address, unit).doubleValue(),
                        h3Core.cellArea(h3address, AreaUnit.valueOf(unit)),
                        1e-8));
    }

    private static boolean almostEqual(double a, double b, double eps) {
        return Math.abs(a - b) < eps;
    }
}
