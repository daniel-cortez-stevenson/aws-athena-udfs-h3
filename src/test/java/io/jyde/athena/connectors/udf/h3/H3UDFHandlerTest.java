/*-
 * #%L
 * h3-udfs
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

import com.uber.h3core.H3Core;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class H3UDFHandlerTest
{
    private static final String DUMMY_H3_ADDRESS = "8b1f9ed32101fff";

    private H3UDFHandler h3UDFHandler;

    @Before
    public void setup()
    {
        H3Core h3 = mock(H3Core.class);
        when(h3.geoToH3Address(0., 0., 11)).thenReturn(DUMMY_H3_ADDRESS);
        this.h3UDFHandler = new H3UDFHandler(h3);
    }

    @Test
    public void geotoh3addressReturnsMock()
    {
        assertEquals(h3UDFHandler.geotoh3address(0., 0., 11), DUMMY_H3_ADDRESS);
    }

    // FIXME: Why does this test pass? Should be assertTrue that passes :(
    // @Test
    // public void h3isvalidForRealH3()
    // {
    //     assertFalse(h3UDFHandler.h3isvalid(DUMMY_H3_ADDRESS));
    // }
}
