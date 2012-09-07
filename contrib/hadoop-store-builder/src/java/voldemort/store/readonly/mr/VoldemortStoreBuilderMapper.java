package voldemort.store.readonly.mr;

import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;

import voldemort.serialization.json.JsonTypeSerializer;
import voldemort.store.readonly.mr.azkaban.StoreBuilderTransformation;
import voldemort.store.readonly.mr.utils.HadoopUtils;
import azkaban.common.utils.Props;
import azkaban.common.utils.Utils;

public class VoldemortStoreBuilderMapper extends AbstractHadoopStoreBuilderMapper<Object, Object> {

    private String _keySelection;
    private String _valSelection;
    private JsonTypeSerializer _inputKeySerializer;
    private JsonTypeSerializer _inputValueSerializer;
    private StoreBuilderTransformation _keyTrans;
    private StoreBuilderTransformation _valTrans;

    @Override
    public Object makeKey(Object key, Object value) {
        return makeResult((BytesWritable) key, _inputKeySerializer, _keySelection, _keyTrans);
    }

    @Override
    public Object makeValue(Object key, Object value) {
        return makeResult((BytesWritable) value, _inputValueSerializer, _valSelection, _valTrans);
    }

    private Object makeResult(BytesWritable writable,
                              JsonTypeSerializer serializer,
                              String selection,
                              StoreBuilderTransformation trans) {
        Object obj = serializer.toObject(writable.get());
        if(selection != null) {
            Map m = (Map) obj;
            obj = m.get(selection);
        }

        if(trans != null)
            obj = trans.transform(obj);

        return obj;
    }

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        Props props = HadoopUtils.getPropsFromJob(conf);

        _keySelection = props.getString("key.selection", null);
        _valSelection = props.getString("value.selection", null);
        _inputKeySerializer = getSchemaFromJob(conf, "mapper.input.key.schema");
        _inputValueSerializer = getSchemaFromJob(conf, "mapper.input.value.schema");
        String _keyTransClass = props.getString("key.transformation.class", null);
        String _valueTransClass = props.getString("value.transformation.class", null);

        if(_keyTransClass != null)
            _keyTrans = (StoreBuilderTransformation) Utils.callConstructor(_keyTransClass);
        if(_valueTransClass != null)
            _valTrans = (StoreBuilderTransformation) Utils.callConstructor(_valueTransClass);
    }

    protected JsonTypeSerializer getSchemaFromJob(JobConf conf, String key) {
        if(conf.get(key) == null)
            throw new IllegalArgumentException("Missing required parameter '" + key + "' on job.");
        return new JsonTypeSerializer(conf.get(key));
    }

}
