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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A strategy based on merging the objects in the list
 * 
 * @author jay
 * 
 */
public class MergingInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    private final ObjectMerger<T> merger;

    public MergingInconsistencyResolver(ObjectMerger<T> merger) {
        this.merger = merger;
    }

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        if(items.size() <= 1) {
            return items;
        } else {
            Iterator<Versioned<T>> iter = items.iterator();
            Versioned<T> current = iter.next();
            T merged = current.getValue();
            VectorClock clock = (VectorClock) current.getVersion();
            while(iter.hasNext()) {
                Versioned<T> versioned = iter.next();
                merged = merger.merge(merged, versioned.getValue());
                clock = clock.merge((VectorClock) versioned.getVersion());
            }
            return Collections.singletonList(new Versioned<T>(merged, clock));
        }
    }
}
