package voldemort.client.protocol.admin;

import java.io.Serializable;

import voldemort.VoldemortException;
import voldemort.utils.Props;
import voldemort.utils.UndefinedPropertyException;

public class StreamingClientConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 10000;
    private static final int DEFAULT_THROTTLE_QPS = 3000;
    // By default, don't tolerate any faulty hosts.
    private static final int DEFAULT_MAX_FAULTY_NODES = 0;
    private static final boolean DEFAULT_OVERWRITE_IF_LATEST_TS = false; 

    private int batchSize;
    private int throttleQPS;
    private int failedNodesTolerated;
    private String bootstrapURL;    
    private boolean overWriteIfLatestTs;

    public StreamingClientConfig() {

    }

    public StreamingClientConfig(Props props) {

        this.batchSize = props.getInt("streaming.platform.commit.batch", DEFAULT_BATCH_SIZE);
        this.throttleQPS = props.getInt("streaming.platform.throttle.qps", DEFAULT_THROTTLE_QPS);
        this.overWriteIfLatestTs = props.getBoolean("streaming.platform.overwrite.if.latest.ts", 
                                                    DEFAULT_OVERWRITE_IF_LATEST_TS);
        this.setFailedNodesTolerated(props.getInt("streaming.platform.max.failed.nodes",
                                                  DEFAULT_MAX_FAULTY_NODES));

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
        
        // TODO For streaming services that use slop as a mechanism to eventually consolidate data across servers, 
        // they should
        // 1. Either disable overwrite_if_latest_ts and go with vector clock based resolution
        // 2. Or add the time based resolving functionality to the Slop pusher on the Voldemort server.
        if (failedNodesTolerated > 0 && overWriteIfLatestTs){
            throw new IllegalArgumentException("Cannot write slops and resolve based on time, at the same time."+
                                                " To move on either set streaming.platform.max.failed.nodes = 0 or " +
                                                " set streaming.platform.overwrite.if.latest.ts=false");
        }
    }

    public int getFailedNodesTolerated() {
        return failedNodesTolerated;
    }

    public void setFailedNodesTolerated(int failedNodesTolerated) {
        this.failedNodesTolerated = failedNodesTolerated;
    }
    
    public boolean isOverWriteIfLatestTs() {
        return overWriteIfLatestTs;
    }

    
    public void setOverWriteIfLatestTs(boolean overWriteIfLatestTs) {
        this.overWriteIfLatestTs = overWriteIfLatestTs;
    }
}
