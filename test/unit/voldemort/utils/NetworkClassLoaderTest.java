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

import java.io.IOException;

import junit.framework.TestCase;

public class NetworkClassLoaderTest extends TestCase {

    NetworkClassLoader networkClassLoader;

    @Override
    public void setUp() {
        networkClassLoader = new NetworkClassLoader(this.getClass().getClassLoader());
    }

    public void testPrimitiveLoadClass() throws IOException {
        // check for simple classes
        try {
            checkLoadClass(new Integer(1).getClass());
            checkLoadClass(new String().getClass());

            fail();
        } catch(SecurityException e) {
            // this is good classLoader fails loading java specific classes.
        }
    }

    public void testUserDefinedClass() throws IOException {
        class UserClass {

            Integer a;
            String b;

            @SuppressWarnings("unused")
            private void foo() {}

            @SuppressWarnings("unused")
            public int getA() {
                return a;
            }

            @SuppressWarnings("unused")
            public String getB() {
                return b;
            }
        }

        checkLoadClass(new UserClass().getClass());
    }

    private void checkLoadClass(Class<?> cl) throws IOException {
        byte[] classBytes = networkClassLoader.dumpClass(cl);

        // ObjectInputStream oin = new ObjectInputStream(new
        // ByteArrayInputStream(classBytes));

        assertEquals("original class should match class after loading.",
                     cl.getName(),
                     networkClassLoader.loadClass(cl.getName(), classBytes, 0, classBytes.length)
                                       .getName());
    }
}
