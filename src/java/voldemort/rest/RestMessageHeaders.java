package voldemort.rest;

/**
 * Commonly used Http headers
 * 
 * 
 */
public class RestMessageHeaders {

    public static final String X_VOLD_REQUEST_TIMEOUT_MS = "X-VOLD-Request-Timeout-ms";
    public static final String X_VOLD_INCONSISTENCY_RESOLVER = "X-VOLD-Inconsistency-Resolver";
    public static final String X_VOLD_VECTOR_CLOCK = "X-VOLD-Vector-Clock";
    public static final String X_VOLD_REQUEST_ORIGIN_TIME_MS = "X-VOLD-Request-Origin-Time-ms";
    public static final String X_VOLD_ROUTING_TYPE_CODE = "X-VOLD-Routing-Type-Code";
    public static final String X_VOLD_GET_VERSION = "X-VOLD-Get-Version";
    public static final String X_VOLD_ZONE_ID = "X-VOLD-Zone-Id";
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_TYPE = "Content-Type";

    // Other headers specific to Voldemort protocol
    public static final String SCHEMATA_STORE = "schemata";
}
