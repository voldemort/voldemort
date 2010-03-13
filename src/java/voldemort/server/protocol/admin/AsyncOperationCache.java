/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort.server.protocol.admin;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Extends LinkedHashMap so that only <em>completed</em> operations may be removed from
 * the map.
 */
public class AsyncOperationCache extends LinkedHashMap<Integer,AsyncOperation> {
    private final int maxSize;
    private static final long serialVersionUID = 1;
    
    /**
     * Create a new cache for background operations.
     * 
     * @param maxSize Maximum size of repository
     */
    public AsyncOperationCache(int maxSize) {
        super(maxSize, 0.75f, false);
        this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer,AsyncOperation> entry) {
        AsyncOperation operation = entry.getValue();
        
        return size() > maxSize() && operation.getStatus().isComplete() ;
    }

    public int maxSize () {
        return maxSize;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AsyncOperationCache(maxSize = ");
        builder.append(maxSize());
        builder.append(", size = ");
        builder.append(size());
        builder.append("[");
        for(AsyncOperation operation: values()) {
            builder.append(operation.toString());
            builder.append(",\n");
        }
        builder.append("])");

        return builder.toString();
    }
}
