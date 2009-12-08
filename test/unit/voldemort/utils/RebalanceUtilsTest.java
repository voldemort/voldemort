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

import java.util.Arrays;

import junit.framework.TestCase;
import voldemort.client.rebalance.RebalanceStealInfo;

public class RebalanceUtilsTest extends TestCase {

    public void testRebalanceStealInfo() {
        RebalanceStealInfo info = new RebalanceStealInfo(0,
                                                         1,
                                                         Arrays.asList(1, 2, 3, 4),
                                                         Arrays.asList("test1", "test2"),
                                                         0);
        System.out.println("info:" + info.toString());

        assertEquals("RebalanceStealInfo fromString --> toString should match.",
                     info.toString(),
                     (new RebalanceStealInfo(info.toString())).toString());
    }
}
