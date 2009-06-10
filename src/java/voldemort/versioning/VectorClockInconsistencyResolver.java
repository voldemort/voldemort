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
import java.util.List;

import com.google.common.collect.Lists;

/**
 * An inconsistency resolver that uses the object VectorClocks leaving only a
 * set of concurrent versions remaining.
 * 
 * @author jay
 * 
 */
public class VectorClockInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        int size = items.size();
        if(size <= 1)
            return items;

        Collections.sort(items, new Versioned.HappenedBeforeComparator<T>());
        List<Versioned<T>> concurrent = Lists.newArrayList();
        Versioned<T> last = items.get(items.size() - 1);
        concurrent.add(last);
        for(int i = items.size() - 2; i >= 0; i--) {
            Versioned<T> curr = items.get(i);
            if(curr.getVersion().compare(last.getVersion()) == Occured.CONCURRENTLY)
                concurrent.add(curr);
            else
                break;
        }

        return concurrent;
    }

}
