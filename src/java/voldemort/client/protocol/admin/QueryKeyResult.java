package voldemort.client.protocol.admin;

import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Return type of AdminClient.QueryKeys. Intended to ensure the following
 * invariant: .hasValues() == !.hasException()
 */
public class QueryKeyResult {

    private final ByteArray key;
    private final List<Versioned<byte[]>> values;
    private final Exception exception;

    QueryKeyResult(ByteArray key, List<Versioned<byte[]>> values) {
        this.key = key;
        this.values = values;
        this.exception = null;
    }

    QueryKeyResult(ByteArray key, Exception exception) {
        this.key = key;
        this.values = null;
        this.exception = exception;
    }

    public ByteArray getKey() {
        return key;
    }

    /**
     * @return true iff values were returned.
     */
    public boolean hasValues() {
        return (values != null);
    }

    public List<Versioned<byte[]>> getValues() {
        return values;
    }

    /**
     * @return true iff exception occured during queryKeys.
     */
    public boolean hasException() {
        return (exception != null);
    }

    public Exception getException() {
        return exception;
    }
}