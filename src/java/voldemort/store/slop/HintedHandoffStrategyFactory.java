package voldemort.store.slop;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

/**
 * A factory that gets the appropriate {@link HintedHandoffStrategy} for a given
 * {@link HintedHandoffStrategyType}.
 */
public class HintedHandoffStrategyFactory {
    
    public HintedHandoffStrategyFactory() {}

    public HintedHandoffStrategy updateHintedHandoffStrategy(StoreDefinition storeDef,
                                                             Cluster cluster) {
        if(HintedHandoffStrategyType.CONSISTENT_STRATEGY.equals(storeDef.getHintedHandoffStrategyType())) {
            Integer hintPrefListSize = storeDef.getHintPrefListSize();
            if(null == hintPrefListSize) {
                if(cluster.getNumberOfNodes() > 6)
                    hintPrefListSize = cluster.getNumberOfNodes() / 2;
                else
                    hintPrefListSize = cluster.getNumberOfNodes();
            }
            return new ConsistentHandoffStrategy(cluster, hintPrefListSize);
        } else if(HintedHandoffStrategyType.TO_ALL_STRATEGY.equals(storeDef.getHintedHandoffStrategyType())) {
            return new HandoffToAllStrategy(cluster);
        } else {
            throw new VoldemortException("HintedHandoffStrategyType:" + storeDef.getHintedHandoffStrategyType()
                                         + " not handled by " + this.getClass());
        }
    }
}
