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
    public Boolean h3isvalid(Long h3) {
        return h3Core.h3IsValid(h3);
    }

    /** Returns true if this is a valid H3 index. */
    public Boolean h3addressisvalid(String h3address) {
        return h3Core.h3IsValid(h3address);
    }

    /** Returns the base cell number for this index. */
    public Integer h3getbasecell(Long h3) {
        return h3Core.h3GetBaseCell(h3);
    }

    /** Returns the base cell number for this index. */
    public Integer h3addressgetbasecell(String h3address) {
        return h3Core.h3GetBaseCell(h3address);
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3ispentagon(Long h3) {
        return h3Core.h3IsPentagon(h3);
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3addressispentagon(String h3address) {
        return h3Core.h3IsPentagon(h3address);
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
    public Long geotoh3(Double lat, Double lng, Integer res) throws IllegalArgumentException {
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
    public String geotoh3address(Double lat, Double lng, Integer res)
            throws IllegalArgumentException {
        return h3Core.geoToH3Address(lat, lng, res);
    }

    /** Find the latitude, longitude (both in degrees) center point of the cell. */
    public String h3togeo(Long h3) {
        return geoCoordToWKSPoint(h3Core.h3ToGeo(h3));
    }

    /** Find the latitude, longitude (degrees) center point of the cell. */
    public String h3addresstogeo(String h3address) throws IllegalArgumentException {
        return geoCoordToWKSPoint(h3Core.h3ToGeo(h3address));
    }

    /** Find the cell boundary in latitude, longitude (degrees) coordinates for the cell */
    public String h3togeoboundary(Long h3) throws IllegalArgumentException {
        return geoCoordsToWKSPolygon(h3Core.h3ToGeoBoundary(h3));
    }

    /**
     * Find the cell boundary in latitude, longitude (degrees) coordinates for the cell
     *
     * @param h3address h3 address
     * @throws IllegalArgumentException
     */
    public String h3addresstogeoboundary(String h3address) throws IllegalArgumentException {
        return geoCoordsToWKSPolygon(h3Core.h3ToGeoBoundary(h3address));
    }

    /**
     * Neighboring indexes in all directions.
     *
     * @param h3 Origin index
     * @param k Number of rings around the origin
     */
    public List<Long> h3kring(Long h3, Integer k) throws IllegalArgumentException {
        return h3Core.kRing(h3, k);
    }

    /**
     * Neighboring addresses in all directions.
     *
     * @param h3address Origin address
     * @param k Number of rings around the origin
     */
    public List<String> h3addresskring(String h3address, Integer k)
            throws IllegalArgumentException {
        return h3Core.kRing(h3address, k);
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
    public Integer h3addressdistance(String a, String b) throws DistanceUndefinedException {
        return h3Core.h3Distance(a, b);
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
    public Integer h3distance(Long a, Long b) throws DistanceUndefinedException {
        return h3Core.h3Distance(a, b);
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
     * @param startaddress Start index of the line
     * @param endaddress End index of the line
     * @return Indexes making up the line.
     * @throws LineUndefinedException The line could not be computed.
     */
    public List<String> h3addressline(String startaddress, String endaddress)
            throws LineUndefinedException {
        return h3Core.h3Line(startaddress, endaddress);
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
    public List<Long> h3line(Long start, Long end) throws LineUndefinedException {
        return h3Core.h3Line(start, end);
    }

    /**
     * Finds indexes within the given geofence.
     *
     * @param polygon Outline geofence
     * @param polygonholes Geofences of any internal holes
     * @param res Resolution of the desired indexes
     */
    public List<String> polyfillh3address(String polygon, List<String> polygonholes, Integer res)
            throws IllegalArgumentException {
        List<GeoCoord> points = geoCoordsFromWKSPolygon(polygon);
        List<List<GeoCoord>> holes =
                polygonholes.stream()
                        .map(this::geoCoordsFromWKSPolygon)
                        .collect(Collectors.toList());
        return h3Core.polyfillAddress(points, holes, res);
    }

    /**
     * Finds indexes within the given geofence.
     *
     * @param polygon Outline geofence
     * @param polygonholes Geofences of any internal holes
     * @param res Resolution of the desired indexes
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<Long> polyfillh3(String polygon, List<String> polygonholes, Integer res)
            throws IllegalArgumentException {
        List<GeoCoord> points = geoCoordsFromWKSPolygon(polygon);
        List<List<GeoCoord>> holes =
                polygonholes.stream()
                        .map(this::geoCoordsFromWKSPolygon)
                        .collect(Collectors.toList());
        return h3Core.polyfill(points, holes, res);
    }

    /** Returns the resolution of the provided index */
    public Integer h3addressgetresolution(String h3Address) {
        return h3Core.h3GetResolution(h3Address);
    }

    /** Returns the resolution of the provided index */
    public Integer h3getresolution(Long h3) {
        return h3Core.h3GetResolution(h3);
    }

    /**
     * Returns the parent of the index at the given resolution.
     *
     * @param h3 H3 index.
     * @param res Resolution of the parent, <code>0 &lt;= res &lt;= h3GetResolution(h3)</code>
     * @throws IllegalArgumentException Invalid resolution
     */
    public Long h3toparent(Long h3, Integer res) {
        return h3Core.h3ToParent(h3, res);
    }

    /**
     * Returns the parent of the index at the given resolution.
     *
     * @param h3address H3 index.
     * @param res Resolution of the parent, <code>0 &lt;= res &lt;= h3GetResolution(h3)</code>
     * @throws IllegalArgumentException Invalid resolution
     */
    public String h3addresstoparent(String h3address, Integer res) {
        return h3Core.h3ToParentAddress(h3address, res);
    }

    /**
     * Provides the children of the index at the given resolution.
     *
     * @param h3 H3 index.
     * @param childres Resolution of the children
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<Long> h3tochildren(Long h3, Integer childres) throws IllegalArgumentException {
        return h3Core.h3ToChildren(h3, childres);
    }

    /**
     * Provides the children of the index at the given resolution.
     *
     * @param h3address H3 index.
     * @param childres Resolution of the children
     * @throws IllegalArgumentException Invalid resolution
     */
    public List<String> h3addresstochildren(String h3address, int childres)
            throws IllegalArgumentException {
        return h3Core.h3ToChildren(h3address, childres);
    }

    /**
     * Returns the center child at the given resolution.
     *
     * @param h3address Parent H3 index address
     * @param childres Resolution of the child
     * @throws IllegalArgumentException Invalid resolution (e.g. coarser than the parent)
     */
    public String h3addresstocenterchild(String h3address, Integer childres)
            throws IllegalArgumentException {
        return h3Core.h3ToCenterChild(h3address, childres);
    }

    /**
     * Returns the center child at the given resolution.
     *
     * @param h3 Parent H3 index
     * @param childres Resolution of the child
     * @throws IllegalArgumentException Invalid resolution (e.g. coarser than the parent)
     */
    public Long h3tocenterchild(Long h3, Integer childres) throws IllegalArgumentException {
        return h3Core.h3ToCenterChild(h3, childres);
    }


    /**
     * Determines if an index is Class III or Class II.
     *
     * @param h3 H3 index
     * @return <code>true</code> if the index is Class III
     */
    public Boolean h3isresclassiii(Long h3) {
        return h3Core.h3IsResClassIII(h3);
    }

    /**
     * Determines if an index is Class III or Class II.
     *
     * @param h3address H3 index address
     * @return <code>true</code> if the index is Class III
     */
    public Boolean h3addressisresclassiii(String h3address) {
        return h3Core.h3IsResClassIII(h3address);
    }

    /**
     * Converts from <code>long</code> representation of an index to <code>String</code>
     * representation.
     */
    public String h3tostring(Long h3) {
        return h3Core.h3ToString(h3);
    }

    /**
     * Converts from <code>String</code> representation of an index to <code>long</code>
     * representation.
     */
    public Long stringtoh3(String h3address) {
        return h3Core.stringToH3(h3address);
    }

    /**
     * Calculates the area of the given H3 cell.
     *
     * @param h3 Cell to find the area of.
     * @param unit Unit to calculate the area in.
     * @return Cell area in the given units.
     */
    public Double h3area(Long h3, String unit) {
        return h3Core.cellArea(h3, AreaUnit.valueOf(unit));
    }

    /**
     * Calculates the area of the given H3 cell.
     *
     * @param h3address Cell to find the area of.
     * @param unit Unit to calculate the area in.
     * @return Cell area in the given units.
     */
    public Double h3addressarea(String h3address, String unit) {
        return h3Core.cellArea(h3address, AreaUnit.valueOf(unit));
    }

    private String geoCoordToWKSPoint(GeoCoord geoCoord) {
        return String.format("POINT (%f %f)", geoCoord.lng, geoCoord.lat);
    }

    private String geoCoordsToWKSPolygon(List<GeoCoord> geoCoords) {
        return geoCoords.stream()
                .map(geoCoord -> String.format("%f %f", geoCoord.lng, geoCoord.lat))
                .collect(Collectors.joining(", ", "POLYGON ((", "))"));
    }

    /**
     * https://stackoverflow.com/a/5011958
     *
     * @param wksPoint A String representation of a WKS Point in AWS Athena
     * @return An H3Core.util.GeoCoord object
     */
    private GeoCoord geoCoordFromWKSPoint(String wksPoint) {
        Pattern p = Pattern.compile("\\d+(\\.\\d+)?");
        Matcher m = p.matcher(wksPoint);
        m.find();
        double lng = Double.parseDouble(m.group());
        m.find();
        double lat = Double.parseDouble(m.group());
        return new GeoCoord(lat, lng);
    }

    /**
     * https://stackoverflow.com/a/5011958
     *
     * @param wksPolygon A String representation of a WKS Polygon in AWS Athena
     * @return An H3Core.util.GeoCoord object
     */
    private List<GeoCoord> geoCoordsFromWKSPolygon(String wksPolygon) {
        Pattern p = Pattern.compile("\\d+(\\.\\d+)?");
        Matcher m = p.matcher(wksPolygon);
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
