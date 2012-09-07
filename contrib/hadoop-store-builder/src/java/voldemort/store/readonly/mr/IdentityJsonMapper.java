package voldemort.store.readonly.mr;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import voldemort.store.readonly.mr.serialization.JsonMapper;

public class IdentityJsonMapper extends JsonMapper {

    @Override
    public void mapObjects(Object key,
                           Object value,
                           OutputCollector<Object, Object> output,
                           Reporter reporter) throws IOException {
        output.collect(key, value);
    }

}