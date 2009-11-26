package voldemort.client.rebalance;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;

public class RebalanceStealInfo {

    private final static String DELIMITER = "#";
    private final int donorId;
    private final List<Integer> partitionList;
    private int attempt;

    public RebalanceStealInfo(int donorId, List<Integer> partitionList, int attempt) {
        super();
        this.donorId = donorId;
        this.partitionList = partitionList;
        this.attempt = attempt;
    }

    public RebalanceStealInfo(String line) {
        try {
            String[] tokens = line.split(DELIMITER);
            this.donorId = Integer.parseInt(tokens[0].trim());
            this.partitionList = new ArrayList<Integer>();
            String listString = tokens[1].replace("[", "").replace("]", "");
            for(String pString: listString.split(",")) {
                if(pString.trim().length() > 0)
                    partitionList.add(Integer.parseInt(pString.trim()));
            }
            this.attempt = Integer.parseInt(tokens[2].trim());
        } catch(Exception e) {
            throw new VoldemortException("Cannot create RebalanceStealInfo from String:'" + line
                                         + "'");
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
        return "" + donorId + DELIMITER + partitionList + DELIMITER + attempt;
    }

}