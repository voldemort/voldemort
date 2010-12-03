package voldemort.store.slop.strategy;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;

/**
 * A factory that gets the appropriate {@link HintedHandoffStrategy} for a given
 * {@link HintedHandoffStrategyType}.
 */
public class HintedHandoffStrategyFactory {

    private final boolean enableZoneRouting;
    private final int clientZoneId;

    public HintedHandoffStrategyFactory(boolean enableZoneRouting, int clientZoneId) {
        this.enableZoneRouting = enableZoneRouting;
        this.clientZoneId = clientZoneId;
    }

    public HintedHandoffStrategy updateHintedHandoffStrategy(StoreDefinition storeDef,
                                                             Cluster cluster) {
        if(HintedHandoffStrategyType.CONSISTENT_STRATEGY.toDisplay()
                                                        .compareTo(storeDef.getHintedHandoffStrategyType()
                                                                           .toDisplay()) == 0) {
            Integer hintPrefListSize = storeDef.getHintPrefListSize();

            // Default value for hint pref list size = replication factor
            if(null == hintPrefListSize) {
                if(cluster.getNumberOfNodes() == storeDef.getReplicationFactor())
                    hintPrefListSize = storeDef.getReplicationFactor() - 1;
                else
                    hintPrefListSize = storeDef.getReplicationFactor();
            }

            return new ConsistentHandoffStrategy(cluster,
                                                 hintPrefListSize,
                                                 enableZoneRouting,
                                                 clientZoneId);
        } else if(HintedHandoffStrategyType.ANY_STRATEGY.toDisplay()
                                                        .compareTo(storeDef.getHintedHandoffStrategyType()
                                                                           .toDisplay()) == 0) {
            return new HandoffToAnyStrategy(cluster, enableZoneRouting, clientZoneId);
        } else if(HintedHandoffStrategyType.PROXIMITY_STRATEGY.toDisplay()
                                                              .compareTo(storeDef.getHintedHandoffStrategyType()
                                                                                 .toDisplay()) == 0) {
            return new ProximityHandoffStrategy(cluster, clientZoneId);
        } else {
            throw new VoldemortException("HintedHandoffStrategyType:"
                                         + storeDef.getHintedHandoffStrategyType()
                                         + " not handled by " + this.getClass());
        }
    }
}
