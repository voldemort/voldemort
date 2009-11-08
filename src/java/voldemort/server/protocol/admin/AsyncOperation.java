package voldemort.server.protocol.admin;

/**
 * @author afeinberg
 */
public abstract class AsyncOperation implements Runnable {
    private final String id;
    private boolean complete=false;
    private String status;

    public AsyncOperation(String id) {
        this.id = id;
    }

    public String getId() { return id; }

    public synchronized boolean getComplete() {
        return complete;
    }
    
    public synchronized void setComplete() {
        this.complete = true;
    }

    public synchronized String getStatus() {
        return status;
    }

    public synchronized void setStatus(String status) {
        this.status = status;
    }
}
