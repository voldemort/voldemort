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
 * An inconsistency resolution strategy that always prefers the first of the two
 * objects.
 * 
 * @author jay
 * 
 */
public class ArbitraryInconsistencyResolver<T> implements InconsistencyResolver<T> {

    /**
     * Arbitrarily resolve the inconsistency by choosing the first object if
     * there is one.
     * 
     * @param values The list of objects
     * @return A single value, if one exists, taken from the input list.
     */
    public List<T> resolveConflicts(List<T> values) {
        if(values.size() > 1)
            return values;
        else
            return Collections.singletonList(values.get(0));
    }

}
