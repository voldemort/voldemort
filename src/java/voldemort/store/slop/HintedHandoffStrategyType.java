package voldemort.store.slop;

/**
 * An enumeration of HintedHandoffStrategy type
 */
public class HintedHandoffStrategyType {

    public static String CONSISTENT_STRATEGY = "consistent-handoff";
    public static String TO_ALL_STRATEGY = "all-handoff";

    private final String name;

    private HintedHandoffStrategyType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
