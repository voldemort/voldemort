package voldemort.server;

import java.util.concurrent.ThreadPoolExecutor;

public class StatusManager {

    private final ThreadPoolExecutor threadPool;
    private final long originalStartTime;

    public StatusManager(ThreadPoolExecutor threadPool) {
        this.threadPool = threadPool;
        this.originalStartTime = System.currentTimeMillis();
    }

    public long getWorkerPoolSize() {
        return threadPool.getPoolSize();
    }

    public long getActiveWorkersCount() {
        return threadPool.getActiveCount();
    }

    public long getUptime() {
        return (System.currentTimeMillis() - originalStartTime) / 1000;
    }

    public String getFormattedUptime() {
        long time = getUptime();
        long days = time / 86400;
        long hours = (time / 3600) - (days * 24);
        long minutes = (time / 60) - (days * 1440) - (hours * 60);
        long seconds = time % 60;
        return String.format("%d days, %d hours, %d minutes, %d seconds",
                             days,
                             hours,
                             minutes,
                             seconds);
    }

}