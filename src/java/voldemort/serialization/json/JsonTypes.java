package voldemort.serialization.json;

import voldemort.serialization.SerializationException;

/**
 * An enum of JsonTypes (for convenience)
 * 
 * @author jay
 * 
 */
public enum JsonTypes {
    BOOLEAN("boolean"),
    STRING("string"),
    INT8("int8"),
    INT16("int16"),
    INT32("int32"),
    INT64("int64"),
    FLOAT32("float32"),
    FLOAT64("float64"),
    BYTES("bytes"),
    DATE("date");

    private final String display;

    private JsonTypes(String display) {
        this.display = display;
    }

    public String toDisplay() {
        return display;
    }

    public static JsonTypes fromDisplay(String name) {
        for (JsonTypes t : JsonTypes.values())
            if (t.toDisplay().equals(name))
                return t;
        throw new SerializationException(name + " is not a valid display for any SimpleType.");
    }

}
