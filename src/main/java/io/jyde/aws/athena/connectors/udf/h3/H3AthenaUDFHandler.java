/* Licensed under Apache-2.0 2021. */
package io.jyde.aws.athena.connectors.udf.h3;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.google.common.annotations.VisibleForTesting;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3Core;
import com.uber.h3core.exceptions.DistanceUndefinedException;
import com.uber.h3core.exceptions.LineUndefinedException;
import com.uber.h3core.util.GeoCoord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class H3AthenaUDFHandler extends UserDefinedFunctionHandler {
    private static final String SOURCE_TYPE = "io.jyde.aws";

    private final H3Core h3Core;

    public H3AthenaUDFHandler() throws IOException {
        super(SOURCE_TYPE);
        this.h3Core = H3Core.newInstance();
    }

    @VisibleForTesting
    H3AthenaUDFHandler(H3Core h3Core) {
        super(SOURCE_TYPE);
        this.h3Core = h3Core;
    }

    /** Returns true if this is a valid H3 index. */
    public Boolean h3_is_valid(Long h3) {
        return h3Core.h3IsValid(h3);
    }

    /** Returns true if this is a valid H3 index. */
    public Boolean h3_is_valid(String h3_address) {
        return h3Core.h3IsValid(h3_address);
    }

    /** Returns the base cell number for this index. */
    public Integer h3_get_base_cell(Long h3) {
        return h3Core.h3GetBaseCell(h3);
    }

    /** Returns the base cell number for this index. */
    public Integer h3_get_base_cell(String h3_address) {
        return h3Core.h3GetBaseCell(h3_address);
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3_is_pentagon(Long h3) {
        return h3Core.h3IsPentagon(h3);
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3_is_pentagon(String h3_address) {
        return h3Core.h3IsPentagon(h3_address);
    }

    /**
     * Find the H3 index of the resolution <code>res</code> cell containing the lat/lon (in degrees)
     *
     * @param lat Latitude in degrees.
     * @param lng Longitude in degrees.
     * @param res Resolution, 0 &lt;= res &lt;= 15
     * @return The H3 index.
     * @throws IllegalArgumentException latitude, longitude, or resolution are out of range.
     */
    public Long geo_to_h3(Double lat, Double lng, Integer res) throws IllegalArgumentException {
        return h3Core.geoToH3(lat, lng, res);
    }

    /**
     * Find the H3 index of the resolution <code>res</code> cell containing the lat/lon (in degrees)
     *
     * @param lat Latitude in degrees.
     * @param lng Longitude in degrees.
     * @param res Resolution, 0 &lt;= res &lt;= 15
     * @return The H3 index.
     * @throws IllegalArgumentException Latitude, longitude, or resolution is out of range.
     */
    public String geo_to_h3_address(Double lat, Double lng, Integer res)
            throws IllegalArgumentException {
        return h3Core.geoToH3Address(lat, lng, res);
    }

    /** Find the latitude, longitude (both in degrees) center point of the cell. */
    public String h3_to_geo(Long h3) {
        return geoCoordToWKTPoint(h3Core.h3ToGeo(h3));
    }

    /** Find the latitude, longitude (degrees) center point of the cell. */
    public String h3_to_geo(String h3_address) throws IllegalArgumentException {
        return geoCoordToWKTPoint(h3Core.h3ToGeo(h3_address));
    }

    /** Find the cell boundary in latitude, longitude (degrees) coordinates for the cell */
    public String h3_to_geo_boundary(Long h3) throws IllegalArgumentException {
        return geoCoordsToWKTPolygon(h3Core.h3ToGeoBoundary(h3));
    }

    /**
     * Find the cell boundary in latitude, longitude (degrees) coordinates for the cell
     *
     * @param h3address h3 address
     * @throws IllegalArgumentException
     */
    public String h3_to_geo_boundary(String h3_address) throws IllegalArgumentException {
        return geoCoordsToWKTPolygon(h3Core.h3ToGeoBoundary(h3_address));
    }

    /**
     * Neighboring indexes in all directions.
     *
     * @param h3 Origin index
     * @param k Number of rings around the origin
     */
    public List<Long> k_ring(Long h3, Integer k) throws IllegalArgumentException {
        return h3Core.kRing(h3, k);
    }

    /**
     * Neighboring addresses in all directions.
     *
     * @param h3address Origin address
     * @param k Number of rings around the origin
     */
    public List<String> k_ring(String h3_address, Integer k) throws IllegalArgumentException {
        return h3Core.kRing(h3_address, k);
    }

    /**
     * Returns the distance between <code>a</code> and <code>b</code>. This is the grid distance, or
     * distance expressed in number of H3 cells.
     *
     * <p>In some cases H3 cannot compute the distance between two indexes. This can happen because:
     *
     * <ul>
     *   <li>The indexes are not comparable (difference resolutions, etc)
     *   <li>The distance is greater than the H3 core library supports
     *   <li>The H3 library does not support finding the distance between the two cells, because of
     *       pentagonal distortion.
     * </ul>
     *
     * @param a An H3 index
     * @param b Another H3 index
     * @return Distance between the two in grid cells
     * @throws DistanceUndefinedException H3 cannot compute the distance.
     */
    public Integer h3_distance(Long a, Long b) {
        if (a == null || b == null) {
            return null;
        }
        try {
            return h3Core.h3Distance(a, b);
        } catch (DistanceUndefinedException e) {
            if (h3Core.h3GetResolution(a) != h3Core.h3GetResolution(b)) {
                throw new RuntimeException(
                        "The H3 library does not support finding the distance between the two cells with different resolutions.",
                        e);
            }
            return null;
        }
    }

    /**
     * Returns the distance between <code>a</code> and <code>b</code>. This is the grid distance, or
     * distance expressed in number of H3 cells.
     *
     * <p>In some cases H3 cannot compute the distance between two indexes. This can happen because:
     *
     * <ul>
     *   <li>The indexes are not comparable (difference resolutions, etc)
     *   <li>The distance is greater than the H3 core library supports
     *   <li>The H3 library does not support finding the distance between the two cells, because of
     *       pentagonal distortion.
     * </ul>
     *
     * @param a An H3 index
     * @param b Another H3 index
     * @return Distance between the two in grid cells
     * @throws DistanceUndefinedException H3 cannot compute the distance.
     */
    public Integer h3_distance(String a, String b) {
        if (a == null || b == null) {
            return null;
        }
        try {
            return h3Core.h3Distance(a, b);
        } catch (DistanceUndefinedException e) {
            if (h3Core.h3GetResolution(a) != h3Core.h3GetResolution(b)) {
                throw new RuntimeException(
                        "The H3 library does not support finding the distance between the two cells with different resolutions.",
                        e);
            }
            return null;
        }
    }

    /**
     * Given two H3 indexes, return the line of indexes between them (inclusive of endpoints).
     *
     * <p>This function may fail to find the line between two indexes, for example if they are very
     * far apart. It may also fail when finding distances for indexes on opposite sides of a
     * pentagon.
     *
     * <p>Notes:
     *
     * <ul>
     *   <li>The specific output of this function should not be considered stable across library
     *       versions. The only guarantees the library provides are that the line length will be
     *       `h3Distance(start, end) + 1` and that every index in the line will be a neighbor of the
     *       preceding index.
     *   <li>Lines are drawn in grid space, and may not correspond exactly to either Cartesian lines
     *       or great arcs.
     * </ul>
     *
     * @param start Start index of the line
     * @param end End index of the line
     * @return Indexes making up the line.
     * @throws LineUndefinedException The line could not be computed.
     */
    public List<Long> h3_line(Long start, Long end) {
        if (start == null || end == null) {
            return null;
        }
        try {
            return h3Core.h3Line(start, end);
        } catch (LineUndefinedException e) {
            return null;
        }
    }

    /**
     * Given two H3 indexes, return the line of indexes between them (inclusive of endpoints).
     *
     * <p>This function may fail to find the line between two indexes, for example if they are very
     * far apart. It may also fail when finding distances for indexes on opposite sides of a
     * pentagon.
     *
     * <p>Notes:
     *
     * <ul>
     *   <li>The specific output of this function should not be considered stable across library
     *       versions. The only guarantees the library provides are that the line length will be
     *       `h3Distance(start, end) + 1` and that every index in the line will be a neighbor of the
     *       preceding index.
     *   <li>Lines are drawn in grid space, and may not correspond exactly to either Cartesian lines
     *       or great arcs.
     * </ul>
     *
     * @param start_address Start index of the line
     * @param end_address End index of the line
     * @return Indexes making up the line.
     * @throws LineUndefinedException The line could not be computed.
     */
    public List<String> h3_line(String start_address, String end_address) {
        if (start_address == null || end_address == null) {
            return null;
        }
        try {
            return h3Core.h3Line(start_address, end_address);
        } catch (LineUndefinedException e) {
            return null;
        }
    }

    /**
     * Finds indexes within the given geofence.
     *
     * @param points Outline geofence
     * @param holes Geofences of any internal holes
     * @param res Resolution of the desired indexes
     */
    public List<String> polyfill_address(String points, List<String> holes, Integer res)
            throws IllegalArgumentException {
        List<GeoCoord> geoCoordPoints = geoCoordsFromWKTPolygon(points);
        List<List<GeoCoord>> geoCoordHoles =
                holes.stream().map(this::geoCoordsFromWKTPolygon).collect(Collectors.toList());
        return h3Core.polyfillAddress(geoCoordPoints, geoCoordHoles, res);
    }

    /**
     * Finds indexes within the given geofence.
     *
     * @param points Outline geofence
     * @param holes Geofences of any internal holes
     * @param res Resolution of the desired indexes
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<Long> polyfill(String points, List<String> holes, Integer res)
            throws IllegalArgumentException {
        List<GeoCoord> geoCoordPoints = geoCoordsFromWKTPolygon(points);
        List<List<GeoCoord>> geoCoordHoles =
                holes.stream().map(this::geoCoordsFromWKTPolygon).collect(Collectors.toList());
        return h3Core.polyfill(geoCoordPoints, geoCoordHoles, res);
    }

    /**
     * Returns the resolution of the provided index
     *
     * @param h3 H3 index.
     */
    public Integer h3_get_resolution(Long h3) {
        return h3Core.h3GetResolution(h3);
    }

    /**
     * Returns the resolution of the provided index
     *
     * @param h3_address H3 index.
     */
    public Integer h3_get_resolution(String h3_address) {
        return h3Core.h3GetResolution(h3_address);
    }

    /**
     * Returns the parent of the index at the given resolution.
     *
     * @param h3 H3 index.
     * @param res Resolution of the parent, <code>0 &lt;= res &lt;= h3GetResolution(h3)</code>
     * @throws IllegalArgumentException Invalid resolution
     */
    public Long h3_to_parent(Long h3, Integer res) throws IllegalArgumentException {
        return h3Core.h3ToParent(h3, res);
    }

    /**
     * Returns the parent of the index at the given resolution.
     *
     * @param h3_address H3 index.
     * @param res Resolution of the parent, <code>0 &lt;= res &lt;= h3GetResolution(h3)</code>
     * @throws IllegalArgumentException Invalid resolution
     */
    public String h3_to_parent(String h3_address, Integer res) throws IllegalArgumentException {
        return h3Core.h3ToParentAddress(h3_address, res);
    }

    /**
     * Provides the children of the index at the given resolution.
     *
     * @param h3 H3 index.
     * @param child_res Resolution of the children
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<Long> h3_to_children(Long h3, Integer child_res) throws IllegalArgumentException {
        return h3Core.h3ToChildren(h3, child_res);
    }

    /**
     * Provides the children of the index at the given resolution.
     *
     * @param h3_address H3 index.
     * @param child_res Resolution of the children
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<String> h3_to_children(String h3_address, Integer child_res)
            throws IllegalArgumentException {
        return h3Core.h3ToChildren(h3_address, child_res);
    }

    /**
     * Returns the center child at the given resolution.
     *
     * @param h3address Parent H3 index address
     * @param child_res Resolution of the child
     * @throws IllegalArgumentException Invalid resolution (e.g. coarser than the parent)
     */
    public String h3_to_center_child(String h3, Integer child_res) throws IllegalArgumentException {
        return h3Core.h3ToCenterChild(h3, child_res);
    }

    /**
     * Returns the center child at the given resolution.
     *
     * @param h3 Parent H3 index
     * @param child_res Resolution of the child
     * @throws IllegalArgumentException Invalid resolution (e.g. coarser than the parent)
     */
    public Long h3_to_center_child(Long h3, Integer child_res) throws IllegalArgumentException {
        return h3Core.h3ToCenterChild(h3, child_res);
    }

    /**
     * Determines if an index is Class III or Class II.
     *
     * @param h3 H3 index
     * @return <code>true</code> if the index is Class III
     */
    public Boolean h3_is_res_class_iii(Long h3) {
        return h3Core.h3IsResClassIII(h3);
    }

    /**
     * Determines if an index is Class III or Class II.
     *
     * @param h3_address H3 index address
     * @return <code>true</code> if the index is Class III
     */
    public Boolean h3_is_res_class_iii(String h3_address) {
        return h3Core.h3IsResClassIII(h3_address);
    }

    /**
     * Converts from <code>long</code> representation of an index to <code>String</code>
     * representation.
     *
     * @param h3 H3 index
     * @return H3 index address
     */
    public String h3_to_string(Long h3) {
        return h3Core.h3ToString(h3);
    }

    /**
     * Converts from <code>String</code> representation of an index to <code>long</code>
     * representation.
     *
     * @param h3_address H3 index address
     * @return H3 index
     */
    public Long string_to_h3(String h3_address) {
        return h3Core.stringToH3(h3_address);
    }

    /**
     * Calculates the area of the given H3 cell.
     *
     * @param h3 Cell to find the area of.
     * @param unit Unit to calculate the area in.
     * @return Cell area in the given units.
     */
    public Double cell_area(Long h3, String unit) {
        return h3Core.cellArea(h3, AreaUnit.valueOf(unit));
    }

    /**
     * Calculates the area of the given H3 cell.
     *
     * @param h3_address Cell to find the area of.
     * @param unit Unit to calculate the area in.
     * @return Cell area in the given units.
     */
    public Double cell_area(String h3_address, String unit) {
        return h3Core.cellArea(h3_address, AreaUnit.valueOf(unit));
    }

    private String geoCoordToWKTPoint(GeoCoord geoCoord) {
        return String.format("POINT (%f %f)", geoCoord.lng, geoCoord.lat);
    }

    private String geoCoordsToWKTPolygon(List<GeoCoord> geoCoords) {
        return geoCoords.stream()
                .map(geoCoord -> String.format("%f %f", geoCoord.lng, geoCoord.lat))
                .collect(Collectors.joining(", ", "POLYGON ((", "))"));
    }

    /**
     * https://stackoverflow.com/a/5011958
     *
     * @param WKTPoint A String representation of a WKT Point in AWS Athena
     * @return An H3Core.util.GeoCoord object
     */
    private GeoCoord geoCoordFromWKTPoint(String WKTPoint) {
        Pattern p = Pattern.compile("\\d+(\\.\\d+)?");
        Matcher m = p.matcher(WKTPoint);
        m.find();
        double lng = Double.parseDouble(m.group());
        m.find();
        double lat = Double.parseDouble(m.group());
        return new GeoCoord(lat, lng);
    }

    /**
     * https://stackoverflow.com/a/5011958
     *
     * @param WKTPolygon A String representation of a WKT Polygon in AWS Athena
     * @return An H3Core.util.GeoCoord object
     */
    private List<GeoCoord> geoCoordsFromWKTPolygon(String WKTPolygon) {
        Pattern p = Pattern.compile("\\d+(\\.\\d+)?");
        Matcher m = p.matcher(WKTPolygon);
        List<GeoCoord> geoCoords = new ArrayList<GeoCoord>();
        while (m.find()) {
            double lng = Double.parseDouble(m.group());
            m.find();
            double lat = Double.parseDouble(m.group());
            geoCoords.add(new GeoCoord(lat, lng));
        }
        return geoCoords;
    }
}
