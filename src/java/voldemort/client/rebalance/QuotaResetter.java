package voldemort.client.rebalance;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import voldemort.client.protocol.admin.AdminClient;
import voldemort.store.metadata.MetadataStore;
import voldemort.tools.admin.AdminToolUtils;

import com.google.common.collect.Maps;


public class QuotaResetter {

    final AdminClient adminClient;
    final Set<Integer> nodeIds;
    final Set<String> storeNames;

    Map<Integer, Boolean> mapNodeToQuotaEnforcingEnabled = Maps.newHashMap();

    public QuotaResetter(AdminClient adminClient, Set<String> storeNames, Set<Integer> nodeIds) {
        this.adminClient = adminClient;
        this.nodeIds = nodeIds;
        this.storeNames = storeNames;
        this.mapNodeToQuotaEnforcingEnabled = Maps.newHashMap();
    }

    public QuotaResetter(String bootstrapUrl, Set<String> storeNames, Set<Integer> nodeIds) {
        this(AdminToolUtils.getAdminClient(bootstrapUrl), storeNames, nodeIds);
    }

    /**
     * Before cluster management operations, i.e. remember and disable quota
     * enforcement settings
     */
    public void rememberAndDisableQuota() {
        for(Integer nodeId: nodeIds) {
            boolean quotaEnforcement = Boolean.parseBoolean(adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                                          MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                                                       .getValue());
            mapNodeToQuotaEnforcingEnabled.put(nodeId, quotaEnforcement);
        }
        adminClient.metadataMgmtOps.updateRemoteMetadata(nodeIds,
                                                         MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                         Boolean.toString(false));
    }

    /**
     * After cluster management operations, i.e. reset quota and recover quota
     * enforcement settings
     */
    public void resetQuotaAndRecoverEnforcement() {
        for(Integer nodeId: nodeIds) {
            boolean quotaEnforcement = mapNodeToQuotaEnforcingEnabled.get(nodeId);
            adminClient.metadataMgmtOps.updateRemoteMetadata(Arrays.asList(nodeId),
                                                             MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                             Boolean.toString(quotaEnforcement));
        }
        for(String storeName: storeNames) {
            adminClient.quotaMgmtOps.rebalanceQuota(storeName);
        }
    }
}
