package voldemort.client;

public enum RoutingTier {
    CLIENT("client"),
    SERVER("server");

    private final String text;

    private RoutingTier(String text) {
        this.text = text;
    }

    public static RoutingTier fromDisplay(String type) {
        for (RoutingTier t : RoutingTier.values())
            if (t.toDisplay().equals(type))
                return t;
        throw new IllegalArgumentException("No RoutingPolicy " + type + " exists.");
    }

    public String toDisplay() {
        return text;
    }
}
