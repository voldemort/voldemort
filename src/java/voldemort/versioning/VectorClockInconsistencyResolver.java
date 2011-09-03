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

import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.Lists;

/**
 * An inconsistency resolver that uses the object VectorClocks leaving only a
 * set of concurrent versions remaining.
 * 
 * 
 */
public class VectorClockInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        int size = items.size();
        if(size <= 1)
            return items;

        List<Versioned<T>> newItems = Lists.newArrayList();
        for(Versioned<T> v1: items) {
            boolean found = false;
            for(ListIterator<Versioned<T>> it2 = newItems.listIterator(); it2.hasNext();) {
                Versioned<T> v2 = it2.next();
                Occurred compare = v1.getVersion().compare(v2.getVersion());
                if(compare == Occurred.AFTER) {
                    if(found)
                        it2.remove();
                    else
                        it2.set(v1);
                }
                if(compare != Occurred.CONCURRENTLY)
                    found = true;
            }
            if(!found)
                newItems.add(v1);
        }
        return newItems;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return (o != null && getClass() == o.getClass());
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
