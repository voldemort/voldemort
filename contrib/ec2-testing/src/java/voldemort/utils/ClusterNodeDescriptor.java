package voldemort.utils;

import java.util.List;

public class ClusterNodeDescriptor {

    private String hostName;

    private int id;

    private List<Integer> partitions;

    public ClusterNodeDescriptor(String hostName, int id, List<Integer> partitions) {
        this.hostName = hostName;
        this.id = id;
        this.partitions = partitions;
    }

    public String getHostName() {
        return hostName;
    }

    public int getId() {
        return id;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

}
