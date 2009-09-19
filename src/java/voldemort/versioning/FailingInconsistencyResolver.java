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
 * An inconsistency resolver that does not attempt to resolve inconsistencies,
 * but instead just throws an exception if one should occur.
 * 
 * @author jay
 */
public class FailingInconsistencyResolver<T> implements InconsistencyResolver<T> {

    public List<T> resolveConflicts(List<T> items) {
        if(items.size() > 1)
            throw new InconsistentDataException("Conflict resolution failed.", items);
        else
            return items;
    }

    public boolean requiresValue() {
        return false;
    }

}
