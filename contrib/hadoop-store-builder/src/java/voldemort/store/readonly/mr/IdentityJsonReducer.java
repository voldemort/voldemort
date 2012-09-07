package voldemort.store.readonly.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.store.readonly.mr.serialization.JsonReducer;

public class IdentityJsonReducer extends JsonReducer {

    @Override
    public void reduceObjects(Object key,
                              Iterator<Object> values,
                              OutputCollector<Object, Object> collector,
                              Reporter reporter) throws IOException {
        while(values.hasNext()) {
            collector.collect(key, values.next());
        }
    }
}
