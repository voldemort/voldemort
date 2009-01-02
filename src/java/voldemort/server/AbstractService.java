package voldemort.server;

import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanOperationInfo;

import org.apache.log4j.Logger;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxOperation;

/**
 * A helper template for implementing VoldemortService
 * 
 * @author jay
 * 
 */
public abstract class AbstractService implements VoldemortService {

    private static final Logger logger = Logger.getLogger(VoldemortService.class);

    private AtomicBoolean isStarted;
    private final String name;

    public AbstractService(String name) {
        this.name = name;
        this.isStarted = new AtomicBoolean(false);
    }

    public String getName() {
        return name;
    }

    @JmxGetter(name = "started", description = "Determine if the service has been started.")
    public boolean isStarted() {
        return isStarted.get();
    }

    @JmxOperation(description = "Start the service.", impact = MBeanOperationInfo.ACTION)
    public void start() {
        boolean isntStarted = isStarted.compareAndSet(false, true);
        if (!isntStarted)
            throw new IllegalStateException("Server is already started!");

        logger.info("Starting " + getName());
        startInner();
    }

    @JmxOperation(description = "Stop the service.", impact = MBeanOperationInfo.ACTION)
    public void stop() {
        logger.info("Stopping " + getName());
        synchronized (this) {
            if (!isStarted()) {
                logger.info("The service is already stopped, ignoring duplicate attempt.");
            }

            stopInner();
            isStarted.set(false);
        }
    }

    protected abstract void startInner();

    protected abstract void stopInner();

}
