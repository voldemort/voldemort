package voldemort.store.readonly.mr.serialization;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import voldemort.serialization.json.JsonTypeSerializer;
import azkaban.common.utils.Utils;

/**
 * Base class for a JsonMapper or JsonReducer with a few basic fields
 * 
 * @author jkreps
 * 
 */
public abstract class JsonConfigurable implements JobConfigurable, Closeable {

    private volatile boolean _isConfigured = false;
    private JsonTypeSerializer _inputKeySerializer;
    private JsonTypeSerializer _inputValueSerializer;
    private JsonTypeSerializer _outputKeySerializer;
    private JsonTypeSerializer _outputValueSerializer;

    public void close() throws IOException {

    }

    public JsonTypeSerializer getInputKeySerializer() {
        return _inputKeySerializer;
    }

    public JsonTypeSerializer getInputValueSerializer() {
        return _inputValueSerializer;
    }

    public JsonTypeSerializer getOutputKeySerializer() {
        return _outputKeySerializer;
    }

    public JsonTypeSerializer getOutputValueSerializer() {
        return _outputValueSerializer;
    }

    protected void setInputKeySerializer(JsonTypeSerializer inputKeySerializer) {
        _inputKeySerializer = Utils.nonNull(inputKeySerializer);
    }

    protected void setInputValueSerializer(JsonTypeSerializer inputValueSerializer) {
        _inputValueSerializer = Utils.nonNull(inputValueSerializer);
    }

    protected void setOutputKeySerializer(JsonTypeSerializer outputKeySerializer) {
        _outputKeySerializer = Utils.nonNull(outputKeySerializer);
    }

    protected void setOutputValueSerializer(JsonTypeSerializer outputValueSerializer) {
        _outputValueSerializer = Utils.nonNull(outputValueSerializer);
    }

    protected void setConfigured(boolean isConfigured) {
        _isConfigured = isConfigured;
    }

    public boolean isConfigured() {
        return _isConfigured;
    }

    protected JsonTypeSerializer getSchemaFromJob(JobConf conf, String key) {
        if(conf.get(key) == null)
            throw new IllegalArgumentException("Missing required parameter '" + key + "' on job.");
        return new JsonTypeSerializer(conf.get(key));
    }

}
