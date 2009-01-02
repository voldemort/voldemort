package voldemort.server;

/**
 * A service that runs in the voldemort server
 * 
 * @author jay
 * 
 */
public interface VoldemortService {

    /**
     * @return The name of this service
     */
    public String getName();

    /**
     * Start the service.
     */
    public void start();

    /**
     * Stop the service
     */
    public void stop();

    /**
     * @return true iff the service is started
     */
    public boolean isStarted();
}
