package voldemort.server.protocol.admin;

/**
 * @author afeinberg
 */
public class AsyncOperationStatus {
    private String status;
    private boolean complete=false;

    private final int id;
    private final String description;

    public AsyncOperationStatus(int id, String description) {
        this.id = id;
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public int getId() {
        return id;
    }
}


