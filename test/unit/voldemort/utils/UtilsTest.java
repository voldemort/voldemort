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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import junit.framework.TestCase;
import voldemort.VoldemortException;

public class UtilsTest extends TestCase {

    public void testSorted() {
        assertEquals(Collections.EMPTY_LIST, Utils.sorted(new ArrayList<Integer>()));
        assertEquals(Arrays.asList(1), Utils.sorted(Arrays.asList(1)));
        assertEquals(Arrays.asList(1, 2, 3, 4), Utils.sorted(Arrays.asList(4, 3, 2, 1)));
    }

    public void testReversed() {
        assertEquals(Collections.EMPTY_LIST, Utils.sorted(new ArrayList<Integer>()));
        assertEquals(Arrays.asList(1, 2, 3, 4), Utils.sorted(Arrays.asList(4, 3, 2, 1)));
    }

    public void testMkDir() {
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "temp"
                                                                      + System.currentTimeMillis());

        // Working case
        Utils.mkdirs(tempDir);
        assertTrue(tempDir.exists());

        // Exists & writable false
        tempDir.setWritable(false);
        try {
            Utils.mkdirs(tempDir);
            fail("Mkdir should have thrown an exception");
        } catch(VoldemortException e) {}

    }

    public void testSymlink() throws IOException {
        String tempParentDir = System.getProperty("java.io.tmpdir");
        File tempDir = new File(tempParentDir, "temp" + (System.currentTimeMillis() + 1));
        File tempSymLink = new File(tempParentDir, "link" + (System.currentTimeMillis() + 2));
        File tempDir2 = new File(tempParentDir, "temp" + (System.currentTimeMillis() + 3));

        // Test against non-existing directory
        assertTrue(!tempDir.exists());
        try {
            Utils.symlink(tempDir.getAbsolutePath(), tempSymLink.getAbsolutePath());
            fail("Symlink should have thrown an exception since directory did not exist");
        } catch(VoldemortException e) {}

        // Normal test
        Utils.mkdirs(tempDir);
        try {
            Utils.symlink(tempDir.getAbsolutePath(), tempSymLink.getAbsolutePath());
        } catch(VoldemortException e) {
            fail("Test against non-existing symlink");
        }

        assertTrue(!Utils.isSymLink(tempDir));
        assertTrue(Utils.isSymLink(tempSymLink));

        // Test if existing sym-link can switch to new directory
        Utils.mkdirs(tempDir2);
        try {
            Utils.symlink(tempDir2.getAbsolutePath(), tempSymLink.getAbsolutePath());
        } catch(VoldemortException e) {
            e.printStackTrace();
            fail("Test against already existing symlink ");
        }
        assertTrue(Utils.isSymLink(tempSymLink));
        // Check if it was not deleted with sym-link
        assertTrue(tempDir.exists());

        File dumbFile = new File(tempDir2, "dumbFile");
        dumbFile.createNewFile();
        assertEquals(1, tempSymLink.list().length);
    }
}
