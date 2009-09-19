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
import java.util.List;

import voldemort.utils.Utils;

/**
 * Apply the given inconsistency resolvers in order until there are 1 or fewer
 * items left.
 * 
 * @author jay
 * 
 */
public class ChainedResolver<T> implements InconsistencyResolver<T> {

    private List<InconsistencyResolver<T>> resolvers;

    public ChainedResolver(InconsistencyResolver<T>... resolvers) {
        this.resolvers = new ArrayList<InconsistencyResolver<T>>(resolvers.length);
        for(InconsistencyResolver<T> resolver: resolvers)
            this.resolvers.add(Utils.notNull(resolver));
    }

    public boolean requiresValue() {
        for(InconsistencyResolver<T> resolver: resolvers) {
            if(resolver.requiresValue())
                return true;
        }
        return false;
    }

    public List<T> resolveConflicts(List<T> items) {
        for(InconsistencyResolver<T> resolver: resolvers) {
            if(items.size() <= 1)
                return items;
            else
                items = resolver.resolveConflicts(items);
        }

        return items;
    }

}
