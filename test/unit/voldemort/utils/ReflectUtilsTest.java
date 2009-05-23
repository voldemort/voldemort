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

package voldemort.utils;

import junit.framework.TestCase;

public class ReflectUtilsTest extends TestCase {

    public void testLoadClass() {
        assertEquals(String.class, ReflectUtils.loadClass(String.class.getName()));
        try {
            ReflectUtils.loadClass("not a class name");
            fail("Loading of bad class name allowed.");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    public void testCallMethod() {
        assertEquals("abcd", ReflectUtils.callMethod("ABCD",
                                                     String.class,
                                                     "toLowerCase",
                                                     new Class<?>[0],
                                                     new Object[0]));
        assertEquals('B', ReflectUtils.callMethod("ABCD",
                                                  String.class,
                                                  "charAt",
                                                  new Class<?>[] { int.class },
                                                  new Object[] { 1 }));
        try {
            ReflectUtils.callMethod("ABCD",
                                    String.class,
                                    "nonExistantMethod",
                                    new Class<?>[0],
                                    new Object[0]);
            fail("Called non-existant method");
        } catch(IllegalArgumentException e) {
            // this is good
        }
    }

    public void testCallConstructor() {
        StringBuilder builder = new StringBuilder("hello");
        assertEquals(new String(builder),
                     ReflectUtils.callConstructor(String.class,
                                                  new Class<?>[] { StringBuilder.class },
                                                  new Object[] { builder }));
    }

    public void testConstruct() {
        String str = "hello";
        String str2 = ReflectUtils.callConstructor(String.class, new Object[] { str });
        assertEquals(str, str2);
    }

}
