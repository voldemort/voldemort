package voldemort.server;

public enum RequestRoutingType {
    NORMAL,
    ROUTED,
    IGNORE_CHECKS;

    /**
     * ignore checks takes precedence over should_route and force it to be local
     * store.
     * 
     * @param should_route
     * @param ignore_checks
     * @return
     */
    public static RequestRoutingType getRequestRoutingType(boolean should_route,
                                                           boolean ignore_checks) {
        if(ignore_checks) {
            return RequestRoutingType.IGNORE_CHECKS;
        } else if(should_route) {
            return RequestRoutingType.ROUTED;
        }

        return RequestRoutingType.NORMAL;
    }
}