package voldemort.server;

/**
 * The various types of voldemort services
 * 
 * @author jay
 * 
 */
public enum ServiceType {
    HTTP("http-service"),
    SOCKET("socket-service"),
    JMX("jmx-service"),
    SCHEDULER("scheduler-service"),
    STORAGE("storage-service"),
    VOLDEMORT("voldemort-server");

    private final String display;

    private ServiceType(String display) {
        this.display = display;
    }

    public String getDisplayName() {
        return this.display;
    }
}
