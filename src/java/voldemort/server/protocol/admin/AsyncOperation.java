package voldemort.server.protocol.admin;

/**
 * @author afeinberg
 */
public abstract class AsyncOperation implements Runnable {
    protected final AsyncOperationStatus status;

    public AsyncOperation(int id, String description) {
        this.status = new AsyncOperationStatus(id, description);
    }

    public synchronized AsyncOperationStatus getStatus() {
        return status;
    }

    public void updateStatus(String msg) {
        synchronized(status) {
            status.setStatus(msg);
        }
    }

    public void markComplete() {
        synchronized(status) {
            status.setComplete(true);
        }
    }

    public void run() {
        updateStatus("started " + getStatus());
        apply();
        updateStatus("finished " + getStatus());
        markComplete();
    }

    abstract public void apply();
}
