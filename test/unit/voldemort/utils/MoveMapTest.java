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

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class MoveMapTest {

    @Test
    public void testBasics() {
        Set<Integer> ids = new HashSet<Integer>();
        for(int i = 0; i < 10; ++i) {
            ids.add(i * i);
        }

        MoveMap mm = new MoveMap(ids);

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                assertTrue(mm.get(from * from, to * to) == 0);
            }
        }

        for(int from = 0; from < 1000; from += 3) {
            int fromId = (from % 10) * (from % 10);
            for(int to = 0; to < 1000; to += 7) {
                int toId = ((from + to) % 10) * ((from + to) % 10);
                mm.increment(fromId, toId);
            }
        }

        // Kind of lame test, but at least its something.
        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                assertTrue(mm.get(from * from, to * to) > 0);
            }
        }

        System.out.println(mm);
    }

    @Test
    public void testAdd() {
        Set<Integer> ids = new HashSet<Integer>();
        for(int i = 0; i < 10; ++i) {
            ids.add(i);
        }

        MoveMap mm1 = new MoveMap(ids);
        MoveMap mm2 = new MoveMap(ids);

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                mm1.increment(from, to);
                mm2.increment(from, to);
                mm2.increment(from, to);
            }
        }

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                assertTrue(mm1.get(from, to) == 1);
                assertTrue(mm2.get(from, to) == 2);
            }
        }

        mm1.add(mm2);

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                assertTrue(mm1.get(from, to) == 3);
            }
        }

    }

    @Test
    public void testGroupBy() {
        Set<Integer> ids = new HashSet<Integer>();
        for(int i = 0; i < 10; ++i) {
            ids.add(i);
        }

        MoveMap mm = new MoveMap(ids);

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                for(int inc = 0; inc < to; ++inc) {
                    mm.increment(from, to);
                }
            }
        }

        for(int from = 0; from < 10; ++from) {
            for(int to = 0; to < 10; ++to) {
                assertTrue(mm.get(from, to) == to);
            }
        }

        Map<Integer, Integer> groupByFrom = mm.groupByFrom();
        for(int id: ids) {
            assertTrue(groupByFrom.get(id) == 45);
        }

        Map<Integer, Integer> groupByTo = mm.groupByTo();
        for(int id: ids) {
            assertTrue(groupByTo.get(id) == id * 10);
        }

        System.out.println(mm.toFlowString());
    }

}
