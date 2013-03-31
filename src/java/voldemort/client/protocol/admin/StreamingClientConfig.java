package voldemort.client.protocol.admin;

import java.io.Serializable;

import voldemort.VoldemortException;
import voldemort.utils.Props;
import voldemort.utils.UndefinedPropertyException;

public class StreamingClientConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int DEFAULT_THROTTLE_QPS = 3000;

    private int batchSize;
    private int throttleQPS;

    private String bootstrapURL;

    public StreamingClientConfig() {

    }

    public StreamingClientConfig(Props props) {

        this.batchSize = props.getInt("streaming.platform.commit.batch", DEFAULT_BATCH_SIZE);
        this.throttleQPS = props.getInt("streaming.platform.throttle.qps", DEFAULT_THROTTLE_QPS);

        try {
            this.bootstrapURL = props.getString("streaming.platform.bootstrapURL");
        } catch(UndefinedPropertyException e) {
            throw new VoldemortException("BootStrap URL Not defined");
        }

        validateParams();

    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getThrottleQPS() {
        return throttleQPS;
    }

    public void setThrottleQPS(int throttleQPS) {
        this.throttleQPS = throttleQPS;
    }

    public String getBootstrapURL() {
        return bootstrapURL;
    }

    public void setBootstrapURL(String bootstrapURL) {
        this.bootstrapURL = bootstrapURL;
    }

    private void validateParams() {

        if(batchSize < 0)
            throw new IllegalArgumentException("streaming.platform.commit.batch cannot be less than 1");

        if(throttleQPS < 0)
            throw new IllegalArgumentException("streaming.platform.throttle.qps cannot be less than 1");

    }
}
