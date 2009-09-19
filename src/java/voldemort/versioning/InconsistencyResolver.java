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

/**
 * A method for resolving inconsistent object values into a single value.
 * Applications can implement this to provide a method for reconciling conflicts
 * that cannot be resolved simply by the version information.
 * 
 * @author jay
 * 
 */
public interface InconsistencyResolver<T> {

    public boolean requiresValue();

    /**
     * Take two different versions of an object and combine them into a single
     * version of the object Implementations must maintain the contract that
     * <ol>
     * <li>
     * {@code resolveConflict([null, null]) == null}</li>
     * <li>
     * if {@code t != null}, then
     * 
     * {@code resolveConflict([null, t]) == resolveConflict([t, null]) == t}</li>
     * 
     * @param items The items to be resolved
     * @return The united object
     */
    public List<T> resolveConflicts(List<T> items);

}
