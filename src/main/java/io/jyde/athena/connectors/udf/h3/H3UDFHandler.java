
/*-
 * #%L
 * H3UDFHandler
 * %%
 * Copyright (C) 2021 Daniel Cortez Stevenson
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.jyde.athena.connectors.udf.h3;

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.google.common.annotations.VisibleForTesting;
import com.uber.h3core.H3Core;

import java.io.IOException;

public class H3UDFHandler
        extends UserDefinedFunctionHandler
{
    private static final String SOURCE_TYPE = "h3_udf_handler";

    private final H3Core h3;

    public H3UDFHandler()
    {
        super(SOURCE_TYPE);
        try {
            this.h3 = H3Core.newInstance();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @VisibleForTesting
    H3UDFHandler(H3Core h3)
    {
        super(SOURCE_TYPE);
        this.h3 = h3;
    }

    /**
     * Returns true if this is a valid H3 index.
     */
    public Boolean h3isvalid(Long h3index) {
        try {
            return h3.h3IsValid(h3index);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns true if this is a valid H3 index.
     */
    public Boolean h3isvalid(String h3address) {
        try {
            return h3.h3IsValid(h3address);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the base cell number for this index.
     */
    public Integer h3getbasecell(Long h3index) {
        try {
            return h3.h3GetBaseCell(h3index);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the base cell number for this index.
     */
    public Integer h3getbasecell(String h3address) {
        try {
            return h3.h3GetBaseCell(h3address);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns <code>true</code> if this index is one of twelve pentagons per resolution.
     */
    public Boolean h3ispentagon(Long h3index) {
        try {
            return h3.h3IsPentagon(h3index);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns <code>true</code> if this index is one of twelve pentagons per resolution.
     */
    public Boolean h3ispentagon(String h3address) {
        try {
            return h3.h3IsPentagon(h3address);
        }
        catch (IllegalArgumentException e) {
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
    public String geotoh3address(Double lat, Double lng, Integer res)
    {
        try {
            return h3.geoToH3Address(lat, lng, res);
        }
        catch (IllegalArgumentException e) {
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
    public Long geotoh3(Double lat, Double lng, Integer res) {
        try {
            return h3.geoToH3(lat, lng, res);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

        /**
     * Find the latitude, longitude (both in degrees) center point of the cell.
     */
    public String h3togeostring(Long h3index) {
        try {
            return h3.h3ToGeo(h3index).toString();
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Find the latitude, longitude (degrees) center point of the cell.
     */
    public String h3togeostring(String h3address) {
        return h3.h3ToGeo(h3address).toString();
    }

}
