package voldemort.store;

import voldemort.client.RoutingTier;
import voldemort.serialization.SerializerDefinition;
import voldemort.store.views.View;
import voldemort.utils.Utils;

/**
 * A simple builder class to avoid having 10k constructor parameters in store
 * definitions
 * 
 * 
 */
public class StoreDefinitionBuilder {

    private String name = null;
    private String type = null;
    private SerializerDefinition keySerializer = null;
    private SerializerDefinition valueSerializer = null;
    private RoutingTier routingPolicy = null;
    private int replicationFactor = -1;
    private Integer preferredWrites = null;
    private int requiredWrites = -1;
    private Integer preferredReads = null;
    private int requiredReads = -1;
    private Integer retentionPeriodDays = null;
    private Integer retentionScanThrottleRate = null;
    private String routingStrategyType = null;
    private String viewOf = null;
    private View<?, ?, ?> view = null;

    public String getName() {
        return Utils.notNull(name);
    }

    public StoreDefinitionBuilder setName(String name) {
        this.name = Utils.notNull(name);
        return this;
    }

    public String getType() {
        return Utils.notNull(type);
    }

    public StoreDefinitionBuilder setType(String type) {
        this.type = Utils.notNull(type);
        return this;
    }

    public SerializerDefinition getKeySerializer() {
        return Utils.notNull(keySerializer);
    }

    public StoreDefinitionBuilder setKeySerializer(SerializerDefinition keySerializer) {
        this.keySerializer = Utils.notNull(keySerializer);
        return this;
    }

    public SerializerDefinition getValueSerializer() {
        return Utils.notNull(valueSerializer);
    }

    public StoreDefinitionBuilder setValueSerializer(SerializerDefinition valueSerializer) {
        this.valueSerializer = Utils.notNull(valueSerializer);
        return this;
    }

    public RoutingTier getRoutingPolicy() {
        return Utils.notNull(routingPolicy);
    }

    public StoreDefinitionBuilder setRoutingPolicy(RoutingTier routingPolicy) {
        this.routingPolicy = Utils.notNull(routingPolicy);
        return this;
    }

    public int getReplicationFactor() {
        return Utils.inRange(replicationFactor, 1, Integer.MAX_VALUE);
    }

    public StoreDefinitionBuilder setReplicationFactor(int replicationFactor) {
        this.replicationFactor = Utils.inRange(replicationFactor, 1, Integer.MAX_VALUE);
        return this;
    }

    public boolean hasPreferredWrites() {
        return preferredWrites != null;
    }

    public Integer getPreferredWrites() {
        return preferredWrites;
    }

    public StoreDefinitionBuilder setPreferredWrites(Integer preferredWrites) {
        this.preferredWrites = preferredWrites;
        return this;
    }

    public int getRequiredWrites() {
        return requiredWrites;
    }

    public StoreDefinitionBuilder setRequiredWrites(int requiredWrites) {
        this.requiredWrites = Utils.inRange(requiredWrites, 0, Integer.MAX_VALUE);
        return this;
    }

    public boolean hasPreferredReads() {
        return preferredReads != null;
    }

    public Integer getPreferredReads() {
        return preferredReads;
    }

    public StoreDefinitionBuilder setPreferredReads(Integer preferredReads) {
        this.preferredReads = preferredReads;
        return this;
    }

    public int getRequiredReads() {
        return requiredReads;
    }

    public StoreDefinitionBuilder setRequiredReads(int requiredReads) {
        this.requiredReads = Utils.inRange(requiredReads, 0, Integer.MAX_VALUE);
        return this;
    }

    public Integer getRetentionPeriodDays() {
        return retentionPeriodDays;
    }

    public StoreDefinitionBuilder setRetentionPeriodDays(Integer retentionPeriodDays) {
        this.retentionPeriodDays = retentionPeriodDays;
        return this;
    }

    public boolean hasRetentionScanThrottleRate() {
        return this.retentionScanThrottleRate != null;
    }

    public Integer getRetentionScanThrottleRate() {
        return retentionScanThrottleRate;
    }

    public StoreDefinitionBuilder setRetentionScanThrottleRate(Integer retentionScanThrottleRate) {
        this.retentionScanThrottleRate = retentionScanThrottleRate;
        return this;
    }

    public String getRoutingStrategyType() {
        return routingStrategyType;
    }

    public StoreDefinitionBuilder setRoutingStrategyType(String routingStrategyType) {
        this.routingStrategyType = Utils.notNull(routingStrategyType);
        return this;
    }

    public boolean isView() {
        return viewOf != null;
    }

    public String getViewOf() {
        return viewOf;
    }

    public StoreDefinitionBuilder setViewOf(String viewOf) {
        this.viewOf = Utils.notNull(viewOf);
        return this;
    }

    public View<?, ?, ?> getView() {
        return view;
    }

    public StoreDefinitionBuilder setView(View<?, ?, ?> valueTransformation) {
        this.view = valueTransformation;
        return this;
    }

    public StoreDefinition build() {
        return new StoreDefinition(this.getName(),
                                   this.getType(),
                                   this.getKeySerializer(),
                                   this.getValueSerializer(),
                                   this.getRoutingPolicy(),
                                   this.getRoutingStrategyType(),
                                   this.getReplicationFactor(),
                                   this.getPreferredReads(),
                                   this.getRequiredReads(),
                                   this.getPreferredWrites(),
                                   this.getRequiredWrites(),
                                   this.getViewOf(),
                                   this.getView(),
                                   this.getRetentionPeriodDays(),
                                   this.getRetentionScanThrottleRate());
    }

}
