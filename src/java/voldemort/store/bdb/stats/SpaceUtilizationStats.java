package voldemort.store.bdb.stats;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.EnvironmentImpl;

/**
 * Obtains the disk space utilization for the BDB environment
 */
final public class SpaceUtilizationStats {

    private final EnvironmentImpl envImpl;

    private SortedMap<Long, FileSummary> summaryMap;
    private long totalSpaceUsed = 0;
    private long totalSpaceUtilized = 0;

    public SpaceUtilizationStats(Environment env) {
        this(DbInternal.getEnvironmentImpl(env));
    }

    private SpaceUtilizationStats(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        UtilizationProfile profile = this.envImpl.getUtilizationProfile();
        summaryMap = profile.getFileSummaryMap(true);

        Iterator<Map.Entry<Long, FileSummary>> fileItr = summaryMap.entrySet().iterator();
        while(fileItr.hasNext()) {
            Map.Entry<Long, FileSummary> entry = fileItr.next();
            FileSummary fs = entry.getValue();
            totalSpaceUsed += fs.totalSize;
            totalSpaceUtilized += fs.totalSize - fs.getObsoleteSize();
        }
    }

    public long getTotalSpaceUsed() {
        return totalSpaceUsed;
    }

    public long getTotalSpaceUtilized() {
        return totalSpaceUtilized;
    }

    public String getSummariesAsString() {
        StringBuffer summaryDetails = new StringBuffer();
        if(summaryMap != null) {
            summaryDetails.append("file,util%\n");
            Iterator<Map.Entry<Long, FileSummary>> fileItr = summaryMap.entrySet().iterator();
            while(fileItr.hasNext()) {
                Map.Entry<Long, FileSummary> entry = fileItr.next();
                FileSummary fs = entry.getValue();
                long bytesUsed = fs.totalSize - fs.getObsoleteSize();
                summaryDetails.append(String.format("%s,%f\n",
                                                    Long.toHexString(entry.getKey().longValue()),
                                                    BdbEnvironmentStats.safeGetPercentage(bytesUsed,
                                                                                          fs.totalSize)));
            }
            summaryDetails.append(String.format("total,%f\n",
                                                BdbEnvironmentStats.safeGetPercentage(totalSpaceUtilized,
                                                                                      totalSpaceUsed)));
        }
        return summaryDetails.toString();
    }
}