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

package voldemort.server.scheduler;

import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.server.AbstractService;
import voldemort.server.ServiceType;
import voldemort.utils.Time;

/**
 * The voldemort scheduler
 * 
 * @author jay
 * 
 */
@JmxManaged(description = "A service that runs scheduled jobs.")
public class SchedulerService extends AbstractService {

    private static final Logger logger = Logger.getLogger(SchedulerService.class.getName());

    private final ScheduledThreadPoolExecutor scheduler;
    private final Time time;

    public SchedulerService(int schedulerThreads, Time time) {
        super(ServiceType.SCHEDULER);
        this.time = time;
        this.scheduler = new SchedulerThreadPool(schedulerThreads);
    }

    @Override
    public void startInner() {}

    @Override
    public void stopInner() {
        this.scheduler.shutdownNow();
    }

    public void scheduleNow(Runnable runnable) {
        scheduler.execute(runnable);
    }

    public void schedule(Runnable runnable, Date timeToRun) {
        scheduler.schedule(runnable, delayMs(timeToRun), TimeUnit.MILLISECONDS);
    }

    public void schedule(Runnable runnable, Date nextRun, long periodMs) {
        scheduler.scheduleAtFixedRate(runnable, delayMs(nextRun), periodMs, TimeUnit.MILLISECONDS);
    }

    private long delayMs(Date runDate) {
        return Math.max(0, runDate.getTime() - time.getMilliseconds());
    }

    /**
     * A scheduled thread pool that fixes some default behaviors
     */
    private static class SchedulerThreadPool extends ScheduledThreadPoolExecutor {

        public SchedulerThreadPool(int numThreads) {
            super(numThreads, new ThreadFactory() {

                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName(r.getClass().getName());
                    return thread;
                }
            });
        }
    }

}
