package voldemort.server.protocol.admin;

/**
 * @author afeinberg
 */
public abstract class AsyncOperation implements Runnable {
    protected boolean complete=false;
    protected String status;

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
