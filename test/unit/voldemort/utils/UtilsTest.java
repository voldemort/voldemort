/*
 * Copyright 2008-2013 LinkedIn, Inc
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import voldemort.VoldemortException;

public class UtilsTest {

    @Test
    public void testSorted() {
        assertEquals(Collections.EMPTY_LIST, Utils.sorted(new ArrayList<Integer>()));
        assertEquals(Arrays.asList(1), Utils.sorted(Arrays.asList(1)));
        assertEquals(Arrays.asList(1, 2, 3, 4), Utils.sorted(Arrays.asList(4, 3, 2, 1)));
    }

    @Test
    public void testReversed() {
        assertEquals(Collections.EMPTY_LIST, Utils.sorted(new ArrayList<Integer>()));
        assertEquals(Arrays.asList(1, 2, 3, 4), Utils.sorted(Arrays.asList(4, 3, 2, 1)));
    }

    @Test
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

    @Test
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

    @Test
    public void testRemoveItemsToSplitListEvenly() {
        // input of size 5
        List<Integer> input = new ArrayList<Integer>();
        System.out.println("Input of size 5");
        for(int i = 0; i < 5; ++i) {
            input.add(i);
        }

        List<Integer> output = Utils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(1), new Integer(3));
        System.out.println("1 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("2 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("3 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("4 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 0);
        System.out.println("5 : " + output);

        // input of size 10
        input.clear();
        System.out.println("Input of size 10");
        for(int i = 0; i < 10; ++i) {
            input.add(i);
        }

        output = Utils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(4), new Integer(9));
        System.out.println("1 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(2), new Integer(8));
        System.out.println("2 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("3 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("4 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("5 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("6 : " + output);

        // input of size 20
        input.clear();
        System.out.println("Input of size 20");
        for(int i = 0; i < 20; ++i) {
            input.add(i);
        }

        output = Utils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 10);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(9), new Integer(19));
        System.out.println("1 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 6);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(5), new Integer(17));
        System.out.println("2 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(4), new Integer(17));
        System.out.println("3 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 4);
        assertEquals(output.get(0), new Integer(4));
        assertEquals(output.get(3), new Integer(16));
        System.out.println("4 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(5));
        assertEquals(output.get(2), new Integer(15));
        System.out.println("5 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("6 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 7);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("7 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 9);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("9 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 10);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("10 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 11);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("11 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 19);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("19 : " + output);

        output = Utils.removeItemsToSplitListEvenly(input, 20);
        assertEquals(output.size(), 0);
        System.out.println("20 : " + output);
    }

    @Test
    public void testPeanutButterList() {

        List<Integer> pbList;

        pbList = Utils.distributeEvenlyIntoList(4, 4);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = Utils.distributeEvenlyIntoList(4, 6);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(2));
        assertEquals(pbList.get(1), new Integer(2));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = Utils.distributeEvenlyIntoList(4, 3);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(0));

        pbList = Utils.distributeEvenlyIntoList(4, 0);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(0));
        assertEquals(pbList.get(1), new Integer(0));
        assertEquals(pbList.get(2), new Integer(0));
        assertEquals(pbList.get(3), new Integer(0));

        boolean caught = false;
        try {
            pbList = Utils.distributeEvenlyIntoList(0, 10);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            pbList = Utils.distributeEvenlyIntoList(4, -5);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);
    }

}
