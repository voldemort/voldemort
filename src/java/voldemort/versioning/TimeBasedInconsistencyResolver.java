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

/**
 * Resolve inconsistencies based on timestamp in the vector clock
 * 
 * @author jay
 * 
 * @param <T> The type f the versioned object
 */
public class TimeBasedInconsistencyResolver<T> implements InconsistencyResolver<Versioned<T>> {

    public List<Versioned<T>> resolveConflicts(List<Versioned<T>> items) {
        if(items.size() <= 1) {
            return items;
        } else {
            Versioned<T> max = items.get(0);
            long maxTime = ((VectorClock) items.get(0).getVersion()).getTimestamp();
            for(Versioned<T> versioned: items) {
                VectorClock clock = (VectorClock) versioned.getVersion();
                if(clock.getTimestamp() > maxTime) {
                    max = versioned;
                    maxTime = ((VectorClock) versioned.getVersion()).getTimestamp();
                }
            }
            return Collections.singletonList(max);
        }
    }
}
