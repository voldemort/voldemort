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

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;

/**
 * 
 * A wrapper class to expose store client factory stats via JMX
 * 
 */

@JmxManaged(description = "Voldemort store client factory")
public class StoreClientFactoryStatsJmx {

    private final StoreClientFactoryStats stats;

    /**
     * Class for JMX
     */
    public StoreClientFactoryStatsJmx(StoreClientFactoryStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "failedBootstrapEvents", description = "Number of failed bootstrap events")
    public int getFailedBootStrapEvents() {
        return stats.getCount(StoreClientFactoryStats.Tracked.FAILED_BOOTSTRAP_EVENT);
    }
    
    @JmxGetter(name = "reBootstrapEvents", description = "Number of rebootstrap events")
    public int getReBootStrapEvents() {
        return stats.getCount(StoreClientFactoryStats.Tracked.REBOOTSTRAP_EVENT);
    }
    
    @JmxGetter(name = "bootstrapEvents", description = "Number of bootstrap events")
    public int getbootStrapEvents() {
        return stats.getCount(StoreClientFactoryStats.Tracked.BOOTSTRAP_EVENT);
    }

}

