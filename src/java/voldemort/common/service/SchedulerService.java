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

package voldemort.common.service;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;
import voldemort.utils.Time;

import com.google.common.collect.Lists;

/**
 * The voldemort scheduler
 * 
 * 
 */
@SuppressWarnings("unchecked")
@JmxManaged(description = "A service that runs scheduled jobs.")
public class SchedulerService extends AbstractService {

    private static final Logger logger = Logger.getLogger(VoldemortService.class);
    private boolean mayInterrupt;

    private class ScheduledRunnable {

        private Runnable runnable;
        private Date delayDate;
        private long intervalMs;

        ScheduledRunnable(Runnable runnable, Date delayDate, long intervalMs) {
            this.runnable = runnable;
            this.delayDate = delayDate;
            this.intervalMs = intervalMs;
        }

        ScheduledRunnable(Runnable runnable, Date delayDate) {
            this(runnable, delayDate, 0);
        }

        Runnable getRunnable() {
            return this.runnable;
        }

        Date getDelayDate() {
            return this.delayDate;
        }

        long getIntervalMs() {
            return this.intervalMs;
        }
    }

    private final ScheduledThreadPoolExecutor scheduler;
    private final Time time;

    private final ConcurrentHashMap<String, ScheduledFuture> scheduledJobResults;
    private final ConcurrentHashMap<String, ScheduledRunnable> allJobs;

    public SchedulerService(int schedulerThreads, Time time) {
        this(schedulerThreads, time, true);
    }

    public SchedulerService(int schedulerThreads, Time time, boolean mayInterrupt) {
        super(ServiceType.SCHEDULER);
        this.time = time;
        this.scheduler = new SchedulerThreadPool(schedulerThreads);
        this.scheduledJobResults = new ConcurrentHashMap<String, ScheduledFuture>();
        this.allJobs = new ConcurrentHashMap<String, ScheduledRunnable>();
        this.mayInterrupt = mayInterrupt;
    }

    @Override
    public void startInner() {}

    @Override
    public void stopInner() {
        this.scheduler.shutdownNow();
    }

    @JmxOperation(description = "Disable a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void disable(String id) {
        if(allJobs.containsKey(id) && scheduledJobResults.containsKey(id)) {
            ScheduledFuture<?> future = scheduledJobResults.get(id);
            boolean cancelled = future.cancel(false);
            if(cancelled == true) {
                logger.info("Removed '" + id + "' from list of scheduled jobs");
                scheduledJobResults.remove(id);
            }
        }
    }

    @JmxOperation(description = "Terminate a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void terminate(String id) {
        if(allJobs.containsKey(id) && scheduledJobResults.containsKey(id)) {
            ScheduledFuture<?> future = scheduledJobResults.get(id);
            boolean cancelled = future.cancel(this.mayInterrupt);
            if(cancelled == true) {
                logger.info("Removed '" + id + "' from list of scheduled jobs");
                scheduledJobResults.remove(id);
            }
        }
    }

    @JmxOperation(description = "Enable a particular scheduled job", impact = MBeanOperationInfo.ACTION)
    public void enable(String id) {
        if(allJobs.containsKey(id) && !scheduledJobResults.containsKey(id)) {
            ScheduledRunnable scheduledRunnable = allJobs.get(id);
            logger.info("Adding '" + id + "' to list of scheduled jobs");
            if(scheduledRunnable.getIntervalMs() > 0) {
                schedule(id,
                         scheduledRunnable.getRunnable(),
                         scheduledRunnable.getDelayDate(),
                         scheduledRunnable.getIntervalMs());
            } else {
                schedule(id, scheduledRunnable.getRunnable(), scheduledRunnable.getDelayDate());
            }

        }
    }

    @JmxGetter(name = "getScheduledJobs", description = "Returns names of jobs in the scheduler")
    public List<String> getScheduledJobs() {
        return Lists.newArrayList(scheduledJobResults.keySet());
    }

    public void scheduleNow(Runnable runnable) {
        scheduler.execute(runnable);
    }

    public void schedule(String id, Runnable runnable, Date timeToRun) {
        ScheduledFuture<?> future = scheduler.schedule(runnable,
                                                       delayMs(timeToRun),
                                                       TimeUnit.MILLISECONDS);
        if(!allJobs.containsKey(id)) {
            allJobs.put(id, new ScheduledRunnable(runnable, timeToRun));
        }
        scheduledJobResults.put(id, future);
    }

    public void schedule(String id, Runnable runnable, Date nextRun, long periodMs) {
        schedule(id, runnable, nextRun, periodMs, false);
    }

    public void schedule(String id,
                         Runnable runnable,
                         Date nextRun,
                         long periodMs,
                         boolean scheduleAtFixedRate) {
        ScheduledFuture<?> future = null;
        if(scheduleAtFixedRate)
            future = scheduler.scheduleAtFixedRate(runnable,
                                                   delayMs(nextRun),
                                                   periodMs,
                                                   TimeUnit.MILLISECONDS);
        else
            future = scheduler.scheduleWithFixedDelay(runnable,
                                                      delayMs(nextRun),
                                                      periodMs,
                                                      TimeUnit.MILLISECONDS);
        if(!allJobs.containsKey(id)) {
            allJobs.put(id, new ScheduledRunnable(runnable, nextRun, periodMs));
        }
        scheduledJobResults.put(id, future);
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
