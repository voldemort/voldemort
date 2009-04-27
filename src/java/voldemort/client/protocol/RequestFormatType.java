package voldemort.client.protocol;

/**
 * An enumeration of request serialization types
 * 
 * @author jay
 * 
 */
public enum RequestFormatType {
    VOLDEMORT("vold"),
    PROTOCOL_BUFFERS("pb");

    private final String name;

    private RequestFormatType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static RequestFormatType fromName(String name) {
        for(RequestFormatType type: RequestFormatType.values())
            if(type.getName().equals(name))
                return type;
        throw new IllegalArgumentException("No wire format '" + name + "' was found");
    }

}
