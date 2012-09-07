package voldemort.store.readonly.mr.utils;

import java.util.Arrays;

import voldemort.serialization.json.JsonTypeDefinition;
import azkaban.common.utils.Utils;

public class JsonSchema {

    private final JsonTypeDefinition key;
    private final JsonTypeDefinition value;

    public JsonSchema(JsonTypeDefinition key, JsonTypeDefinition value) {
        super();
        this.key = Utils.nonNull(key);
        this.value = Utils.nonNull(value);
    }

    public JsonTypeDefinition getKeyType() {
        return key;
    }

    public JsonTypeDefinition getValueType() {
        return value;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new Object[] { key, value });
    }

    @Override
    public boolean equals(Object o) {
        if(o == null)
            return false;
        if(o == this)
            return true;
        if(o.getClass() != JsonSchema.class)
            return false;
        JsonSchema s = (JsonSchema) o;
        return getKeyType().equals(s.getKeyType()) && getValueType().equals(s.getValueType());
    }

    @Override
    public String toString() {
        return key.toString() + " => " + value.toString();
    }

}
