package voldemort.store.stats;

import org.apache.log4j.Logger;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.annotations.jmx.JmxOperation;

@JmxManaged(description = "Streaming related statistics")
public class StreamStatsJmx {
    private final static Logger logger = Logger.getLogger(StreamStatsJmx.class);

    private final StreamStats stats;

    public StreamStatsJmx(StreamStats stats) {
        this.stats = stats;
    }

    @JmxGetter(name = "streamOperationIds", description = "Get a list of all stream operations")
    public String getStreamOperationIds() {
        try {
            return stats.getHandleIds().toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }

    @JmxGetter(name = "allStreamOperations", description = "Get status of all stream operations")
    public String getAllStreamOperations() {
        try {
            return stats.getHandles().toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }

    @JmxOperation(description = "Get the status of a stream operation with specified id")
    public String getStreamOperation(long handleId) {
        try {
            return stats.getHandle(handleId).toString();
        } catch(Exception e) {
            logger.error("Exception in JMX call", e);
            return e.getMessage();
        }
    }
}
