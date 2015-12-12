package voldemort.store.readonly.mr;

import java.io.IOException;

/**
 * Simple abstraction in order to interface with two types of MR collectors:
 * - {@link org.apache.hadoop.mapred.OutputCollector}
 * - {@link org.apache.avro.mapred.AvroCollector}
 */
public abstract class AbstractCollectorWrapper<COLLECTOR> {
    private COLLECTOR collector;

    public void setCollector(COLLECTOR collector) {
        if (this.collector != collector) {
            this.collector = collector;
        }
    }

    protected COLLECTOR getCollector() {
        return this.collector;
    }

    public abstract void collect(byte[] key, byte[] value) throws IOException;
}
