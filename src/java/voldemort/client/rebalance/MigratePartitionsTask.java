package voldemort.client.rebalance;

import java.util.List;

public class MigratePartitionsTask {

    private final int donorId;
    private final int stealerNodeId;
    private final String storeName;
    private final List<Integer> partitionList;

    public MigratePartitionsTask(int donorId,
                                 int stealerNodeId,
                                 String storeName,
                                 List<Integer> partitionList) {
        this.donorId = donorId;
        this.stealerNodeId = stealerNodeId;
        this.storeName = storeName;
        this.partitionList = partitionList;
    }

    public int getDonorId() {
        return donorId;
    }

    public int getStealerNodeId() {
        return stealerNodeId;
    }

    public String getStoreName() {
        return storeName;
    }

    public List<Integer> getPartitionList() {
        return partitionList;
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;

        MigratePartitionsTask that = (MigratePartitionsTask) o;

        if(donorId != that.donorId) return false;
        if(stealerNodeId != that.stealerNodeId) return false;
        if(partitionList != null ? !partitionList
                .equals(that.partitionList) : that.partitionList != null) return false;
        if(storeName != null ? !storeName.equals(that.storeName) : that.storeName != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = donorId;
        result = 31 * result + stealerNodeId;
        result = 31 * result + (storeName != null ? storeName.hashCode() : 0);
        result = 31 * result + (partitionList != null ? partitionList.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MigratePartitionsTask(" +
               "donorId=" + donorId +
               ", stealerNodeId=" + stealerNodeId +
               ", storeName='" + storeName + '\'' +
               ", partitionList=" + partitionList +
               ')';
    }
}
