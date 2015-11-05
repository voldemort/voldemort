package voldemort.store.readonly.mr;

import java.io.IOException;

/**
 * Simple abstraction in order to interface with two types of MR collectors:
 * - {@link org.apache.hadoop.mapred.OutputCollector}
 * - {@link org.apache.avro.mapred.AvroCollector}
 */
public interface AbstractCollector {
    public void collect(byte[] key, byte[] value) throws IOException;
}
