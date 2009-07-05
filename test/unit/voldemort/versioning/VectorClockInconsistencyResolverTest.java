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

package voldemort.versioning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import voldemort.TestUtils;

public class VectorClockInconsistencyResolverTest extends TestCase {

    private InconsistencyResolver<Versioned<String>> resolver;
    private Versioned<String> later;
    private Versioned<String> prior;
    private Versioned<String> current;
    private Versioned<String> concurrent;
    private Versioned<String> concurrent2;

    @Override
    public void setUp() {
        resolver = new VectorClockInconsistencyResolver<String>();
        current = getVersioned(1, 1, 2, 3);
        prior = getVersioned(1, 2, 3);
        concurrent = getVersioned(1, 2, 3, 3);
        concurrent2 = getVersioned(1, 2, 3, 4);
        later = getVersioned(1, 1, 2, 2, 3);
    }

    private Versioned<String> getVersioned(int... nodes) {
        return new Versioned<String>("my-value", TestUtils.getClock(nodes));
    }

    public void testEmptyList() {
        assertEquals(0, resolver.resolveConflicts(new ArrayList<Versioned<String>>()).size());
    }

    @SuppressWarnings("unchecked")
    public void testDuplicatesResolve() {
        assertEquals(2, resolver.resolveConflicts(Arrays.asList(concurrent,
                                                                current,
                                                                current,
                                                                concurrent,
                                                                current)).size());
    }

    @SuppressWarnings("unchecked")
    public void testResolveNormal() {
        assertEquals(later, resolver.resolveConflicts(Arrays.asList(current, prior, later)).get(0));
        assertEquals(later, resolver.resolveConflicts(Arrays.asList(prior, current, later)).get(0));
        assertEquals(later, resolver.resolveConflicts(Arrays.asList(later, current, prior)).get(0));
    }

    @SuppressWarnings("unchecked")
    public void testResolveConcurrent() {
        List<Versioned<String>> resolved = resolver.resolveConflicts(Arrays.asList(current,
                                                                                   concurrent,
                                                                                   prior));
        assertEquals(2, resolved.size());
        assertTrue("Version not found", resolved.contains(current));
        assertTrue("Version not found", resolved.contains(concurrent));
    }

    @SuppressWarnings("unchecked")
    public void testResolveLargerConcurrent() {
        assertEquals(3, resolver.resolveConflicts(Arrays.asList(concurrent,
                                                                concurrent2,
                                                                current,
                                                                concurrent2,
                                                                current,
                                                                concurrent,
                                                                current)).size());
    }
}
