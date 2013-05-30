package voldemort.coordinator;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.VoldemortException;
import voldemort.annotations.jmx.JmxGetter;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.store.InsufficientZoneResponsesException;
import voldemort.store.InvalidMetadataException;
import voldemort.store.routed.PipelineRoutedStats;

/**
 * Class to keep track of all the errors in the Coordinator service
 * 
 */
public class CoordinatorErrorStats extends PipelineRoutedStats {

    CoordinatorErrorStats() {
        super();
        this.errCountMap.put(RejectedExecutionException.class, new AtomicLong(0));
        this.errCountMap.put(IllegalArgumentException.class, new AtomicLong(0));
        this.errCountMap.put(VoldemortException.class, new AtomicLong(0));
    }

    @Override
    public boolean isSevere(Exception ve) {
        if(ve instanceof InsufficientOperationalNodesException
           || ve instanceof InsufficientZoneResponsesException
           || ve instanceof InvalidMetadataException || ve instanceof RejectedExecutionException
           || ve instanceof IllegalArgumentException || ve instanceof VoldemortException)
            return true;
        else
            return false;
    }

    @JmxGetter(name = "numRejectedExecutionExceptions", description = "Number of rejected tasks by the Fat client")
    public long getNumRejectedExecutionExceptions() {
        return errCountMap.get(RejectedExecutionException.class).get();
    }

    @JmxGetter(name = "numIllegalArgumentExceptions", description = "Number of bad requests received by the Coordinator")
    public long getNumIllegalArgumentExceptions() {
        return errCountMap.get(IllegalArgumentException.class).get();
    }

    @JmxGetter(name = "numVoldemortExceptions", description = "Number of failed Voldemort operations")
    public long getNumVoldemortExceptions() {
        return errCountMap.get(VoldemortException.class).get();
    }

}
