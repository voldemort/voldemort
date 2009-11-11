package voldemort.server.protocol.admin;

/**
 * @author afeinberg
 */
public abstract class AsyncOperation implements Runnable {
    protected final AsyncOperationStatus status;

    public AsyncOperation(int id, String description) {
        status = new AsyncOperationStatus(id, description);
    }

    public AsyncOperationStatus getStatus() {
        return status;
    }
    
    public void run() {
        status.setStatus("Started " + status.getDescription());
        apply();
        status.setStatus("Finished " + status.getDescription());
        status.setComplete(true);
    }


    abstract public void apply();
}
