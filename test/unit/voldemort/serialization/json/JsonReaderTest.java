/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.serialization.json;

import static voldemort.TestUtils.quote;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;
import voldemort.serialization.SerializationException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * @author jay
 * 
 */
public class JsonReaderTest extends TestCase {

    public void testObjects() throws IOException {
        assertParsed(ImmutableMap.of("1", 1, "2", 2), "{\"1\":1, \"2\":2}");
        assertParsed(ImmutableMap.of("1", 1, "2", 2), "{\"1\"   : 1, \"2\": 2}");
        assertParsed(ImmutableMap.of(), "{}");
        assertParsed(ImmutableMap.of("h", ImmutableMap.of()), "{\"h\":{}}");
    }

    public void testArrays() throws IOException {
        assertParsed(ImmutableList.of(), "[]");
        assertParsed(ImmutableList.of(1), "[1]");
        assertParsed(ImmutableList.of(1, ImmutableList.of(1)), "[1, [1]]");
    }

    public void testStrings() throws IOException {
        assertParsed("hello", quote("hello"));
        assertParsed(" hello ", quote(" hello "));
        assertParsed(" hello \n\r", quote(" hello \n\r"));
        assertParsed("", quote(""));
        assertParsed("\"", "'\"'");

        // test strings with single quotes
        assertParsed(" hello \n\r", "' hello \n\r'");
        assertParsed("\u1234", "'\\u1234'");

        // control sequences
        assertParsed("\"", quote("\\\""));
        assertParsed("\\", quote("\\\\"));
        assertParsed("/", quote("\\/"));
        assertParsed("\b", quote("\\b"));
        assertParsed("\f", quote("\\f"));
        assertParsed("\n", quote("\\n"));
        assertParsed("\r", quote("\\r"));
        assertParsed("\t", quote("\\t"));

        // unicode literals
        assertParsed("\u1234", quote("\\u1234"));
        assertParsed("\u12aF", quote("\\u12aF"));
        assertParsed("\u12Af", quote("\\u12Af"));
    }

    public void testNumbers() throws IOException {
        assertParsed(-1, "-1");
        assertParsed(1, "1");
        assertParsedDouble(1.1d, "1.1");
        assertParsedDouble(0.12304d, "0.12304");
        assertParsedDouble(1e10d, "1e10");
        assertParsedDouble(1e10d, "1e+10");
        assertParsedDouble(-1e-10d, "-1E-10");
    }

    public void testNull() throws IOException {
        assertParsed(null, "null");
    }

    public void testBoolean() throws IOException {
        assertParsed(true, "true");
        assertParsed(false, "false");
    }

    public void testInvalid() throws IOException {
        assertFails("1a");
        assertFails("[2");
        assertFails("{");
        assertFails("}");
        assertFails("-[]");
        assertFails("pkldjf");
        assertFails("1ef");
        assertFails("1E");
        assertFails(quote("\\u123v"));
    }

    public void testParseMultiple() throws Exception {
        JsonReader reader = new JsonReader(new StringReader("1{} {} [2]\"3\" "));
        assertTrue(reader.hasMore());
        assertEquals(1, reader.read());
        assertTrue(reader.hasMore());
        assertEquals(ImmutableMap.of(), reader.read());
        assertTrue(reader.hasMore());
        assertEquals(ImmutableMap.of(), reader.read());
        assertTrue(reader.hasMore());
        assertEquals(ImmutableList.of(2), reader.read());
        assertTrue(reader.hasMore());
        assertEquals("3", reader.read());
        assertTrue(!reader.hasMore());
    }

    public void testComplex() throws Exception {
        assertParsed(ImmutableList.of(ImmutableMap.of("",
                                                      false,
                                                      "LxO45",
                                                      true,
                                                      "l1jBAJTSO",
                                                      ImmutableList.of(3245847,
                                                                       4,
                                                                       ImmutableList.of(),
                                                                       3988)),
                                      ImmutableMap.of(),
                                      ImmutableMap.of("", "sk7QzU"),
                                      "0lMsJq"), "[{\"\":false, \"LxO45\":true,"
                                                 + " \"l1jBAJTSO\":[3245847, 4, [], 3988]}, "
                                                 + " {}, {\"\":\"sk7QzU\"}, \"0lMsJq\"]");
    }

    private void assertFails(String value) throws IOException {
        try {
            new JsonReader(new StringReader(value)).read();
            fail("Invalid value \"" + value + "\" parsed without error.");
        } catch(SerializationException e) {
            // good
        }
    }

    private void assertParsedDouble(Double d1, String value) throws IOException {
        Object o = new JsonReader(new StringReader(value)).read();
        assertEquals(Double.class, o.getClass());
        Double d2 = (Double) o;
        assertEquals(d1.doubleValue(), d2.doubleValue(), 0.00001);
    }

    private void assertParsed(Object expected, String value) throws IOException {
        assertEquals(expected, new JsonReader(new StringReader(value)).read());
        // add some random whitespace
        assertEquals(expected, new JsonReader(new StringReader("  " + value + "  ")).read());
    }

}
