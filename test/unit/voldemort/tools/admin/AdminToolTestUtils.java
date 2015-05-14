package voldemort.tools.admin;

import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;

public class AdminToolTestUtils {

    // This function should only be used in test code path
    public static StoreDefinitionBuilder storeDefToBuilder(StoreDefinition sd) {
        StoreDefinitionBuilder sb = new StoreDefinitionBuilder();
        sb.setName(sd.getName())
          .setDescription(sd.getDescription())
          .setType(sd.getType())
          .setRoutingPolicy(sd.getRoutingPolicy())
          .setRoutingStrategyType(sd.getRoutingStrategyType())
          .setKeySerializer(sd.getKeySerializer())
          .setValueSerializer(sd.getKeySerializer())
          .setReplicationFactor(sd.getReplicationFactor())
          .setZoneReplicationFactor(sd.getZoneReplicationFactor())
          .setRequiredReads(sd.getRequiredReads())
          .setRequiredWrites(sd.getRequiredWrites())
          .setZoneCountReads(sd.getZoneCountReads())
          .setZoneCountWrites(sd.getZoneCountWrites());
        return sb;
    }
}
