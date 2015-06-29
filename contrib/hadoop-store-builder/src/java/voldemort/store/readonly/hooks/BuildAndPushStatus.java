package voldemort.store.readonly.hooks;

/**
 * This enum describes the various stages a Build and Push can be in.
 */
public enum BuildAndPushStatus {
    // The "happy path" status:
    STARTING,               // emitted once per job
    BUILDING,               // emitted once per job
    PUSHING,                // emitted once per cluster
    SWAPPED,                // emitted once per cluster
    FINISHED(true),         // emitted once per job
    HEARTBEAT,              // emitted periodically during any of the stages...

    // The "not-so-happy path" status:
    SWAPPED_WITH_FAILURES,  // emitted for each cluster that failed to push completely but still managed to swap
    CANCELLED(true),        // emitted once per job
    FAILED(true);           // emitted once per job

    private final boolean terminationStatus;
    private BuildAndPushStatus() {
        this(false);
    }
    private BuildAndPushStatus(boolean terminationStatus) {
        this.terminationStatus = terminationStatus;
    }

    public boolean isTerminal() {
        return terminationStatus;
    }
}