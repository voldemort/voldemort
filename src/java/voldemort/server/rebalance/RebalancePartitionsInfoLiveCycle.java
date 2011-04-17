package voldemort.server.rebalance;

import voldemort.client.rebalance.RebalancePartitionsInfo;
import voldemort.store.metadata.MetadataStore.RebalancePartitionsInfoLifeCycleStatus;

/**
 * Immutable class that keeps track of the running status of a
 * {@link RebalancePartitionsInfo} instance.
 * 
 */
public class RebalancePartitionsInfoLiveCycle {

    private final RebalancePartitionsInfo rebalancePartitionsInfo;
    private final RebalancePartitionsInfoLifeCycleStatus status;

    public RebalancePartitionsInfoLiveCycle(final RebalancePartitionsInfo pinfo,
                                            final RebalancePartitionsInfoLifeCycleStatus status) {
        this.rebalancePartitionsInfo = pinfo;
        this.status = status;
    }

    public RebalancePartitionsInfo getRebalancePartitionsInfo() {
        return rebalancePartitionsInfo;
    }

    public RebalancePartitionsInfoLifeCycleStatus getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null)
            return false;

        if(!(obj instanceof RebalancePartitionsInfoLiveCycle))
            return false;

        RebalancePartitionsInfoLiveCycle other = (RebalancePartitionsInfoLiveCycle) obj;

        if(!this.getStatus().equals(other.getStatus()))
            return false;

        if(!this.getRebalancePartitionsInfo().equals(other.getRebalancePartitionsInfo()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = rebalancePartitionsInfo.hashCode();
        result = 37 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(rebalancePartitionsInfo);
        sb.append(" - (status:").append(getStatus().name()).append(")");
        return sb.toString();
    }
}

