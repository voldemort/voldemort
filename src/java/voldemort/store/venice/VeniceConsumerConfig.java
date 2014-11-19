package voldemort.store.venice;

/**
 * A class which stores the tuning variables needed for initializing a kafka consumer
 */
public class VeniceConsumerConfig {

    public static final int DEFAULT_NUM_RETRIES = 3;
    public static final int DEFAULT_REQUEST_TIMEOUT = 100000;

    public static final int DEFAULT_OFFSET_COMMIT_CYCLE = 5000; // 5 seconds
    public static final int DEFAULT_REQUEST_FETCH_SIZE = 10000000;
    public static final int DEFAULT_REQUEST_BUFFER_SIZE = 1024 * 1024;

    // tuning variables
    private final int numberOfRetriesBeforeFailure;
    private final int requestTimeout;
    private final int requestFetchSize;
    private final int requestBufferSize;
    private final int offsetCommitCycle;

    // Venice metadata location
    private final String offsetMetadataPath;

    /* To be used only in test scenarios */
    public VeniceConsumerConfig() {
        this.numberOfRetriesBeforeFailure = DEFAULT_NUM_RETRIES;
        this.requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        this.requestFetchSize = DEFAULT_REQUEST_FETCH_SIZE;
        this.requestBufferSize = DEFAULT_REQUEST_BUFFER_SIZE;
        this.offsetCommitCycle = DEFAULT_OFFSET_COMMIT_CYCLE;
        this.offsetMetadataPath = "";
    }

    public VeniceConsumerConfig(int numberOfRetriesBeforeFailure,
                                int requestTimeout,
                                int requestFetchSize,
                                int requestBufferSize,
                                int offsetCommitCycle,
                                String offsetMetadataPath) {
        this.numberOfRetriesBeforeFailure = numberOfRetriesBeforeFailure;
        this.requestTimeout = requestTimeout;
        this.requestFetchSize = requestFetchSize;
        this.requestBufferSize = requestBufferSize;
        this.offsetCommitCycle = offsetCommitCycle;
        this.offsetMetadataPath = offsetMetadataPath;
    }

    public int getNumberOfRetriesBeforeFailure() {
        return numberOfRetriesBeforeFailure;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public int getRequestFetchSize() {
        return requestFetchSize;
    }

    public int getRequestBufferSize() {
        return requestBufferSize;
    }

    public int getOffsetCommitCycle() {
        return offsetCommitCycle;
    }

    public String getOffsetMetadataPath() { return offsetMetadataPath; }

}
