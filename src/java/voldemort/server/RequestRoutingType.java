package voldemort.server;

import voldemort.VoldemortException;

public enum RequestRoutingType {
    NORMAL(0),
    ROUTED(1),
    IGNORE_CHECKS(2);

    private final int routingTypeCode;

    private RequestRoutingType(int routingType) {
        this.routingTypeCode = routingType;
    }

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

    public int getRoutingTypeCode() {
        return routingTypeCode;
    }

    /**
     * ignore checks takes precedence over should_route and force it to be local
     * store.
     * 
     * @param should_route
     * @param ignore_checks
     * @return
     */
    public static RequestRoutingType getRequestRoutingType(int routingCode) {
        switch(routingCode) {
            case 0:
                return RequestRoutingType.NORMAL;
            case 1:
                return RequestRoutingType.ROUTED;
            case 2:
                return RequestRoutingType.IGNORE_CHECKS;
        }

        throw new VoldemortException("Invalid RequestRoutingType code passed " + routingCode);
    }
}