package voldemort.server.protocol.admin;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class AsyncOperationStatus {

    private volatile String status = "initializing";
    private AtomicBoolean complete = new AtomicBoolean(false);
    private volatile Exception exception;

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
        return complete.get();
    }

    public void setComplete(boolean complete) {
        this.complete.getAndSet(complete);
    }

    public int getId() {
        return id;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public boolean hasException() {
        return exception != null;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("AsyncOperationStatus(id = ");
        builder.append(getId());
        builder.append(", description = ");
        builder.append(getDescription());
        builder.append(", complete = ");
        builder.append(isComplete());
        builder.append(", status = ");
        builder.append(getStatus());
        builder.append(")");

        return builder.toString();
    }
}
