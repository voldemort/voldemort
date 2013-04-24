/*
 * Copyright 2013 LinkedIn, Inc
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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class RepartitionUtilsTest {

    // TODO: test basic balancing with & without balancing partitions between
    // zone for following use cases: rebalance, cluster expansion, zone
    // expansion.

    // TODO: test breaking up contiguous partition runs within a zone
    // TODO: deal with TODO in RepartitionUtils that reduces number of
    // rebalancing methods.

    // TODO: test rebalance with random swaps

    // TODO: test rebalance with greedy swaps

    @Test
    public void testRemoveItemsToSplitListEvenly() {
        // input of size 5
        List<Integer> input = new ArrayList<Integer>();
        System.out.println("Input of size 5");
        for(int i = 0; i < 5; ++i) {
            input.add(i);
        }

        List<Integer> output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(1), new Integer(3));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(2));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 0);
        System.out.println("5 : " + output);

        // input of size 10
        input.clear();
        System.out.println("Input of size 10");
        for(int i = 0; i < 10; ++i) {
            input.add(i);
        }

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(4), new Integer(9));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(2), new Integer(8));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(1), new Integer(7));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("5 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(5));
        System.out.println("6 : " + output);

        // input of size 20
        input.clear();
        System.out.println("Input of size 20");
        for(int i = 0; i < 20; ++i) {
            input.add(i);
        }

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 1);
        assertEquals(output.size(), 10);
        assertEquals(output.get(0), new Integer(1));
        assertEquals(output.get(9), new Integer(19));
        System.out.println("1 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 2);
        assertEquals(output.size(), 6);
        assertEquals(output.get(0), new Integer(2));
        assertEquals(output.get(5), new Integer(17));
        System.out.println("2 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 3);
        assertEquals(output.size(), 5);
        assertEquals(output.get(0), new Integer(3));
        assertEquals(output.get(4), new Integer(17));
        System.out.println("3 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 4);
        assertEquals(output.size(), 4);
        assertEquals(output.get(0), new Integer(4));
        assertEquals(output.get(3), new Integer(16));
        System.out.println("4 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 5);
        assertEquals(output.size(), 3);
        assertEquals(output.get(0), new Integer(5));
        assertEquals(output.get(2), new Integer(15));
        System.out.println("5 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 6);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("6 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 7);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("7 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 9);
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), new Integer(6));
        assertEquals(output.get(1), new Integer(13));
        System.out.println("9 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 10);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("10 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 11);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("11 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 19);
        assertEquals(output.size(), 1);
        assertEquals(output.get(0), new Integer(10));
        System.out.println("19 : " + output);

        output = RepartitionUtils.removeItemsToSplitListEvenly(input, 20);
        assertEquals(output.size(), 0);
        System.out.println("20 : " + output);
    }

    @Test
    public void testPeanutButterList() {

        List<Integer> pbList;

        pbList = RepartitionUtils.peanutButterList(4, 4);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = RepartitionUtils.peanutButterList(4, 6);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(2));
        assertEquals(pbList.get(1), new Integer(2));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(1));

        pbList = RepartitionUtils.peanutButterList(4, 3);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(1));
        assertEquals(pbList.get(1), new Integer(1));
        assertEquals(pbList.get(2), new Integer(1));
        assertEquals(pbList.get(3), new Integer(0));

        pbList = RepartitionUtils.peanutButterList(4, 0);
        assertEquals(pbList.size(), 4);
        assertEquals(pbList.get(0), new Integer(0));
        assertEquals(pbList.get(1), new Integer(0));
        assertEquals(pbList.get(2), new Integer(0));
        assertEquals(pbList.get(3), new Integer(0));

        boolean caught = false;
        try {
            pbList = RepartitionUtils.peanutButterList(0, 10);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);

        caught = false;
        try {
            pbList = RepartitionUtils.peanutButterList(4, -5);
        } catch(IllegalArgumentException iae) {
            caught = true;
        }
        assertTrue(caught);
    }

}
