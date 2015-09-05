package voldemort.store.readonly.checksum;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSDataInputStream;

import voldemort.serialization.json.JsonReader;
import voldemort.serialization.json.JsonWriter;
import voldemort.store.readonly.ReadOnlyStorageMetadata;

import com.google.common.collect.Maps;

public class CheckSumMetadata {

    public final static String DATA_FILE_SIZE_IN_BYTES = "data_file_size_in_bytes";
    public final static String INDEX_FILE_SIZE_IN_BYTES = "index_file_size_in_bytes";

    private Map<String, Object> properties;

    public CheckSumMetadata() {
        this.properties = new HashMap<String, Object>();
    }

    public CheckSumMetadata(Map<String, Object> prop) {
        this();
        this.properties.putAll(prop);
    }

    public CheckSumMetadata(String json) {
        this();
        JsonReader reader = new JsonReader(new StringReader(json));
        properties.putAll(reader.readObject());
    }

    public CheckSumMetadata(FSDataInputStream checkSumMetadataFile) throws IOException {
        this();
        BufferedReader reader = new BufferedReader(new InputStreamReader(checkSumMetadataFile));
        JsonReader jsonReader = new JsonReader(reader);
        properties.putAll(jsonReader.readObject());
    }

    public String toJsonString() throws IOException {
        StringWriter stringWriter = new StringWriter();
        new JsonWriter(stringWriter).writeMap(properties);
        stringWriter.flush();
        return stringWriter.toString();
    }

    public boolean isEmpty() {
        return properties.isEmpty();
    }

    public void add(String key, String value) {
        properties.put(key, value);
    }

    public void remove(String key) {
        properties.remove(key);
    }

    public Object get(String key) {
        return properties.get(key);
    }

    public byte[] getCheckSum() throws DecoderException {
        String checkSumString = (String) get(ReadOnlyStorageMetadata.CHECKSUM);
        if(checkSumString == null) {
            return null;
        }

        return Hex.decodeHex(checkSumString.toCharArray());
    }

    public Object get(String key, Object defaultValue) {
        if(properties.get(key) == null)
            return defaultValue;
        return properties.get(key);
    }

    public Map<String, Object> getAll() {
        return Maps.newHashMap(properties);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || getClass() != o.getClass())
            return false;

        CheckSumMetadata that = (CheckSumMetadata) o;

        Map<String, Object> thisMap = this.getAll();
        Map<String, Object> thatMap = that.getAll();

        if(thisMap == null && thatMap == null)
            return true;
        else if(thisMap == null || thatMap == null)
            return false;

        for(String key: thisMap.keySet()) {
            Object thisValue = thisMap.get(key);
            Object thatValue = thatMap.get(key);

            if(thisValue == null && thatValue == null)
                continue;
            else if(thisValue == null || thatValue == null)
                return false;

            if(!thisValue.equals(thatValue))
                return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return this.properties.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CheckSumMetadata( ");
        sb.append("\n");
        for(String key: this.properties.keySet()) {
            sb.append(key + " : " + properties.get(key) + ",");
            sb.append("\n");
        }
        sb.append(")");

        return sb.toString();
    }
}
