/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.store.stats;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class StoreClientFactoryStats {
    
    private final Map<Tracked, AtomicInteger> counters;
    
    public StoreClientFactoryStats() {
        counters = new EnumMap<Tracked, AtomicInteger>(Tracked.class);
        for (Tracked tracked: Tracked.values())
            counters.put(tracked, new AtomicInteger(0));
    }

    public int getCount(Tracked metric) {
        return counters.get(metric).get();
    }
    
    public void incrementCount(Tracked metric) {
        counters.get(metric).getAndIncrement();
    }
    
    public static enum Tracked {
        REBOOTSTRAP_EVENT("rebootstrapEvent"),
        FAILED_BOOTSTRAP_EVENT("failedBootstrapEvent"),
        BOOTSTRAP_EVENT("bootstrapEvent");
        
        private final String name;
        private Tracked(String name) {
            this.name = name;
        }
        @Override
        public String toString() {
            return name;
        }
    }
}
