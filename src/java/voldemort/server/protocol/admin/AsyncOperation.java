package voldemort.server.protocol.admin;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;

/**
 */
public abstract class AsyncOperation implements Runnable {

    protected final AsyncOperationStatus status;

    public AsyncOperation(int id, String description) {
        this.status = new AsyncOperationStatus(id, description);
    }

    @JmxGetter(name = "asyncTaskStatus")
    public AsyncOperationStatus getStatus() {
        return status;
    }

    public void updateStatus(String msg) {
        status.setStatus(msg);
    }

    public void markComplete() {
        status.setComplete(true);

    }

    public void run() {
        updateStatus("Started " + getStatus());
        String previousThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(previousThreadName + "; AsyncOp ID " + status.getId());
        try {
            operate();
        } catch(Exception e) {
            status.setException(e);
        } finally {
            Thread.currentThread().setName(previousThreadName);
            updateStatus("Finished " + getStatus());
            markComplete();
        }
    }

    @Override
    public String toString() {
        return getStatus().toString();
    }

    abstract public void operate() throws Exception;

    @JmxOperation
    abstract public void stop();

    /**
     * Cumulative wait time is reported as Seconds in the JMX by AsyncService.
     * AsyncOperation can return a positive number indicating the milliseconds
     * it is waiting, to be included for that calculation.
     *
     * @return If the AsyncOperation is waiting, return the wait time in
     *         milliseconds else return 0
     */
    public long getWaitTimeMs() {
        return 0;
    }

    /**
     * Number of wait tasks is reported as Seconds in the JMX by AsyncService.
     * AsyncOperation can return true to indicate that it should be included in
     * that calculation.
     *
     * @return If the AsyncOperation is waiting, return true else false
     */
    public boolean isWaiting() {
        return false;
    }
}
