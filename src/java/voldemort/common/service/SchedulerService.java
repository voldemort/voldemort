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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
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

    private static final Logger logger = Logger.getLogger(SchedulerService.class);
    private boolean mayInterrupt;
    private static final String THREAD_NAME_PREFIX = "voldemort-scheduler-service";
    private static final AtomicInteger schedulerServiceCount = new AtomicInteger(0);
    private final String schedulerName = THREAD_NAME_PREFIX + schedulerServiceCount.incrementAndGet();

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

    private final Map<String, ScheduledFuture> scheduledJobResults;
    private final Map<String, ScheduledRunnable> allJobs;

    public SchedulerService(int schedulerThreads, Time time) {
        this(schedulerThreads, time, true);
    }

    public SchedulerService(int schedulerThreads, Time time, boolean mayInterrupt) {
        super(ServiceType.SCHEDULER);
        this.time = time;
        this.scheduler = new SchedulerThreadPool(schedulerThreads, schedulerName);
        this.scheduledJobResults = new ConcurrentHashMap<String, ScheduledFuture>();
        this.allJobs = new ConcurrentHashMap<String, ScheduledRunnable>();
        this.mayInterrupt = mayInterrupt;
    }

    @Override
    public void startInner() {
        // TODO note that most code does not do this. so scheduler.isStarted()
        // returns false even after you have submitted some tasks and they are
        // running fine.
    }

    @Override
    public void stopInner() {
        this.scheduler.shutdownNow();
        try {
            this.scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch(Exception e) {
            logger.info("Error waiting for termination of scheduler service", e);
        }
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

    public List<String> getAllJobs() {
        return Lists.newArrayList(allJobs.keySet());
    }

    public boolean getJobEnabled(String id) {
        if(allJobs.containsKey(id)) {
            return scheduledJobResults.containsKey(id);
        } else {
            throw new VoldemortException("Job id "+id + " does not exist.");
        }
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
        public SchedulerThreadPool(int numThreads, final String schedulerName) {
            super(numThreads, new ThreadFactory() {
                private AtomicInteger threadCount = new AtomicInteger(0);

                /**
                 * This function is overridden in order to activate the daemon mode as well as
                 * to give a human readable name to threads used by the {@link SchedulerService}.
                 * 
                 * Previously, this function would set the thread's name to the value of the passed-in
                 * {@link Runnable}'s class name, but this is useless since it always ends up being a
                 * java.util.concurrent.ThreadPoolExecutor$Worker
                 *
                 * Instead, a generic name is now used, and the thread's name can be set more
                 * precisely during {@link voldemort.server.protocol.admin.AsyncOperation#run()}.
                 * 
                 * @param r {@link Runnable} to execute
                 * @return a new {@link Thread} appropriate for use within the {@link SchedulerService}.
                 */
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true);
                    thread.setName(schedulerName + "-t" + threadCount.incrementAndGet());
                    return thread;
                }
            });
        }
    }

    @JmxGetter(name = "numActiveTasks", description = "Returns number of tasks executing currently")
    public long getActiveTasksCount() {
        return this.scheduler.getActiveCount();
    }

    @JmxGetter(name = "numQueuedTasks",
            description = "Returns number of tasks queued for execution")
    public long getQueuedTasksCount() {
        return this.scheduler.getQueue().size();
    }

}
