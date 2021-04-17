/* (C)2021 Daniel Cortez Stevenson*/
package io.jyde.athena.connectors.udf.h3;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.google.common.annotations.VisibleForTesting;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class H3UDFHandler extends UserDefinedFunctionHandler {
    private static final String SOURCE_TYPE = "athena_common_udfs";

    private final H3Core h3;

    public H3UDFHandler() {
        super(SOURCE_TYPE);
        try {
            this.h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    H3UDFHandler(H3Core h3) {
        super(SOURCE_TYPE);
        this.h3 = h3;
    }

    /** Returns true if this is a valid H3 index. */
    public Boolean h3indexisvalid(Long h3index) {
        try {
            return h3.h3IsValid(h3index);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns true if this is a valid H3 index. */
    public Boolean h3addressisvalid(String h3address) {
        try {
            return h3.h3IsValid(h3address);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns the base cell number for this index. */
    public Integer h3indexgetbasecell(Long h3index) {
        try {
            return h3.h3GetBaseCell(h3index);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns the base cell number for this index. */
    public Integer h3addressgetbasecell(String h3address) {
        try {
            return h3.h3GetBaseCell(h3address);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3indexispentagon(Long h3index) {
        try {
            return h3.h3IsPentagon(h3index);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Returns <code>true</code> if this index is one of twelve pentagons per resolution. */
    public Boolean h3addressispentagon(String h3address) {
        try {
            return h3.h3IsPentagon(h3address);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
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
    public Long geotoh3index(Double lat, Double lng, Integer res) {
        try {
            return h3.geoToH3(lat, lng, res);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
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
    public String geotoh3address(Double lat, Double lng, Integer res) {
        try {
            return h3.geoToH3Address(lat, lng, res);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Find the latitude, longitude (both in degrees) center point of the cell. */
    public String h3indextogeo(Long h3index) {
        try {
            GeoCoord geocoord = h3.h3ToGeo(h3index);
            return String.format("POINT (%f %f)", geocoord.lng, geocoord.lat);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Find the latitude, longitude (degrees) center point of the cell. */
    public String h3addresstogeo(String h3address) {
        try {
            GeoCoord geoCoord = h3.h3ToGeo(h3address);
            return String.format("POINT (%f %f)", geoCoord.lng, geoCoord.lat);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Find the cell boundary in latitude, longitude (degrees) coordinates for the cell */
    public String h3indextogeoboundary(Long h3index) {
        try {
            List<String> geoStringList =
                    h3.h3ToGeoBoundary(h3index).stream()
                            .map(
                                    geoCoord ->
                                            String.format(
                                                    "POINT (%f %f)", geoCoord.lng, geoCoord.lat))
                            .collect(Collectors.toList());
            return geoStringList.toString();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /** Find the cell boundary in latitude, longitude (degrees) coordinates for the cell */
    public String h3addresstogeoboundary(String h3address) {
        try {
            List<String> geoStringList =
                    h3.h3ToGeoBoundary(h3address).stream()
                            .map(
                                    geoCoord ->
                                            String.format(
                                                    "POINT (%f %f)", geoCoord.lng, geoCoord.lat))
                            .collect(Collectors.toList());
            return geoStringList.toString();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Neighboring indexes in all directions.
     *
     * @param h3address Origin index
     * @param k Number of rings around the origin
     */
    public String h3addresskring(String h3address, Integer k) {
        try {
            List<String> kRingList = h3.kRing(h3address, k);
            return kRingList.toString();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Neighboring indexes in all directions.
     *
     * @param h3index Origin index
     * @param k Number of rings around the origin
     */
    public String h3indexkring(Long h3index, Integer k) {
        try {
            List<Long> kRingList = h3.kRing(h3index, k);
            return kRingList.toString();
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }
}
