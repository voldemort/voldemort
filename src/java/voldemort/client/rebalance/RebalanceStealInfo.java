package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;

public class RebalanceStealInfo {

    private final int donorId;
    private final List<Integer> partitionList;
    private final String storeName;

    private final static String DELIMITER = "#";

    public String getStoreName() {
        return storeName;
    }

    private int attempt;

    public RebalanceStealInfo(String storeName,
                              int donorId,
                              List<Integer> partitionList,
                              int attempt) {
        super();
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
        this.storeName = storeName;
    }

    public RebalanceStealInfo(String line) {
        try {
            String[] tokens = line.split(DELIMITER);
            this.storeName = tokens[0];
            this.donorId = Integer.parseInt(tokens[1].trim());
            this.partitionList = new ArrayList<Integer>();
            String listString = tokens[2].replace("[", "").replace("]", "");
            for(String pString: listString.split(",")) {
                if(pString.trim().length() > 0)
                    partitionList.add(Integer.parseInt(pString.trim()));
            }
            this.attempt = Integer.parseInt(tokens[3].trim());
        } catch(Exception e) {
            throw new VoldemortException("Cannot create RebalanceStealInfo from String:'" + line
                                         + "'", e);
        }
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public int getDonorId() {
        return donorId;
    }

    public List<Integer> getPartitionList() {
        return partitionList;
    }

    public int getAttempt() {
        return attempt;
    }

    @Override
    public String toString() {
        return "" + storeName + DELIMITER + donorId + DELIMITER + partitionList + DELIMITER
               + attempt;
    }
}