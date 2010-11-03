package voldemort.store.slop.strategy;

/**
 * Enumerates different hinted handoff strategies.
 * 
 * 
 */
public enum HintedHandoffStrategyType {
    CONSISTENT_STRATEGY("consistent-handoff"),
    ANY_STRATEGY("any-handoff"),
    PROXIMITY_STRATEGY("proximity-handoff");

    private final String text;

    private HintedHandoffStrategyType(String text) {
        this.text = text;
    }

    public static HintedHandoffStrategyType fromDisplay(String type) {
        for(HintedHandoffStrategyType t: HintedHandoffStrategyType.values())
            if(t.toDisplay().compareTo(type) == 0)
                return t;
        return null;
    }

    public String toDisplay() {
        return text;
    }
}