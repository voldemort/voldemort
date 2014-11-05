package voldemort.store.venice;

/**
 * A class which stores the tuning variables needed for initializing a kafka consumer
 */
public class VeniceConsumerConfig {

    public static final int DEFAULT_NUM_RETRIES = 3;
    public static final int DEFAULT_REQUEST_TIMEOUT = 100000;
    public static final int DEFAULT_REQUEST_FETCH_SIZE = 100000;
    public static final int DEFAULT_REQUEST_BUFFER_SIZE = 64 * 1024;

    // tuning variables
    private int numberOfRetriesBeforeFailure;
    private int requestTimeout;
    private int requestFetchSize;
    private int requestBufferSize;

    /* To be used only in test scenarios */
    public VeniceConsumerConfig() {
        this.numberOfRetriesBeforeFailure = DEFAULT_NUM_RETRIES;
        this.requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        this.requestFetchSize = DEFAULT_REQUEST_FETCH_SIZE;
        this.requestBufferSize = DEFAULT_REQUEST_BUFFER_SIZE;
    }

    public VeniceConsumerConfig(int numberOfRetriesBeforeFailure,
                                int requestTimeout,
                                int requestFetchSize,
                                int requestBufferSize) {
        this.numberOfRetriesBeforeFailure = numberOfRetriesBeforeFailure;
        this.requestTimeout = requestTimeout;
        this.requestFetchSize = requestFetchSize;
        this.requestBufferSize = requestBufferSize;
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

}
