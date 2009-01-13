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

package voldemort.store.readonly;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;
import voldemort.serialization.StringSerializer;

import com.google.common.collect.Lists;

public class ExternalSorterTest extends TestCase {

    private List<String> abcs;

    public void setUp() {
        abcs = new ArrayList<String>();
        String abcStr = "QERTYUIOLKJHGFDSAMNBVCXZqwertyuiopasghjklzxcvbnm";
        for(int i = 0; i < abcStr.length(); i++)
            abcs.add(abcStr.substring(i, i + 1));
    }

    public void testSorting() {
        ExternalSorter<String> sorter = new ExternalSorter<String>(new StringSerializer(), 10);
        List<String> sorted = Lists.newArrayList(sorter.sorted(abcs.iterator()));
        List<String> expected = new ArrayList<String>(abcs);
        Collections.sort(expected);
        assertEquals(expected, sorted);
    }

}
