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

package voldemort.client;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.utils.DaemonThreadFactory;

/**
 * A thread pool with a more convenient constructor and some jmx monitoring
 * 
 * 
 */
@JmxManaged(description = "A voldemort client thread pool")
public class ClientThreadPool extends ThreadPoolExecutor {

    public ClientThreadPool(int maxThreads, long threadIdleMs, int maxQueuedRequests) {
        super(maxThreads,
              maxThreads,
              threadIdleMs,
              TimeUnit.MILLISECONDS,
              new LinkedBlockingQueue<Runnable>(maxQueuedRequests),
              new DaemonThreadFactory("voldemort-client-thread-"),
              new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @JmxGetter(name = "numberOfActiveThreads", description = "The number of active threads.")
    public int getNumberOfActiveThreads() {
        return this.getActiveCount();
    }

    @JmxGetter(name = "numberOfThreads", description = "The total number of threads, active and idle.")
    public int getNumberOfThreads() {
        return this.getPoolSize();
    }

    @JmxGetter(name = "queuedRequests", description = "Number of requests in the queue waiting to execute.")
    public int getQueuedRequests() {
        return this.getQueue().size();
    }
}
