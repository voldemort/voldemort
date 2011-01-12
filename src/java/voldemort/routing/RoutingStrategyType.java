package voldemort.routing;

/**
 * An enumeration of RoutingStrategies type
 * 
 * 
 */
public class RoutingStrategyType {

    public static String CONSISTENT_STRATEGY = "consistent-routing";
    public static String TO_ALL_STRATEGY = "all-routing";
    public static String ZONE_STRATEGY = "zone-routing";

    private final String name;

    private RoutingStrategyType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
