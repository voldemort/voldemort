package voldemort.common.service;

/**
 * The various types of voldemort services
 * 
 * 
 */
public enum ServiceType {
    HTTP("http-service"),
    SOCKET("socket-service"),
    ADMIN("admin-service"),
    JMX("jmx-service"),
    SCHEDULER("scheduler-service"),
    STORAGE("storage-service"),
    VOLDEMORT("voldemort-server"),
    ASYNC_SCHEDULER("async-scheduler"),
    GOSSIP("gossip-service"),
    REBALANCE("rebalance-service"),
    RESTSERVICE("rest-service"),
    HDFSAuth("hdfs-auth-service"),
    COORDINATOR_PROXY("coordinator-proxy-service"),
    COORDINATOR_ADMIN("coordinator-admin-service"),
    COORDINATOR_SERVER("coordinator-server");

    private final String display;

    private ServiceType(String display) {
        this.display = display;
    }

    public String getDisplayName() {
        return this.display;
    }
}
