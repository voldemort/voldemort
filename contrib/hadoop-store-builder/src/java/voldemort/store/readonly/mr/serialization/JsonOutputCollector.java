package voldemort.store.readonly.mr.serialization;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.OutputCollector;

import voldemort.serialization.Serializer;
import azkaban.common.utils.Utils;

/**
 * Output collector that handles JSON serialization
 * 
 * @author jkreps
 * 
 */
public class JsonOutputCollector<K, V> implements OutputCollector<K, V> {

    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valueSerializer;
    private final OutputCollector<BytesWritable, BytesWritable> innerCollector;

    public JsonOutputCollector(OutputCollector<BytesWritable, BytesWritable> innerCollector,
                               Serializer<Object> keySerializer,
                               Serializer<Object> valueSerializer) {
        this.innerCollector = Utils.nonNull(innerCollector);
        this.keySerializer = Utils.nonNull(keySerializer);
        this.valueSerializer = Utils.nonNull(valueSerializer);
    }

    public void collect(K key, V value) throws IOException {
        innerCollector.collect(new BytesWritable(keySerializer.toBytes(key)),
                               new BytesWritable(valueSerializer.toBytes(value)));
    }

    public Serializer<Object> getKeySerializer() {
        return keySerializer;
    }

    public Serializer<Object> getValueSerializer() {
        return valueSerializer;
    }

}
