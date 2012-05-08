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
 * Code based in the com.sleepycat.je.util.DbSpace class.
 */
final public class DbSpace {

    private final EnvironmentImpl envImpl;
    private Summary totals = new Summary();
    private Summary[] summaries = null;
    private StringBuffer summaryDetails = new StringBuffer();

    public DbSpace(Environment env) {
        this(DbInternal.getEnvironmentImpl(env));
    }

    private DbSpace(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;

        UtilizationProfile profile = this.envImpl.getUtilizationProfile();
        SortedMap<Long, FileSummary> map = profile.getFileSummaryMap(true);

        int fileIndex = 0;

        totals = new Summary();
        summaries = new Summary[map.size()];

        Iterator<Map.Entry<Long, FileSummary>> iter = map.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<Long, FileSummary> entry = iter.next();
            Long fileNum = entry.getKey();
            FileSummary fs = entry.getValue();

            Summary summary = new Summary(fileNum, fs);
            summaries[fileIndex] = summary;

            totals.add(summary);
            fileIndex++;
        }

        summaryDetails.append("  File    Size (KB)  % Used\n--------  ---------  ------\n");
        for(int i = 0; i < summaries.length; ++i) {
            summaryDetails.append(summaries[i].toString());
            summaryDetails.append("\n");
        }

        summaryDetails.append(totals.toString());
    }

    public Summary getTotal() {
        return totals;
    }

    public String getSummariesAsString() {
        return summaryDetails.toString();
    }

    public static class Summary {

        private Long fileNum;
        private long totalSize;
        private long obsoleteSize;

        public Summary() {}

        public Summary(Long fileNum, FileSummary summary) {
            this.fileNum = fileNum;
            this.totalSize = summary.totalSize;
            this.obsoleteSize = summary.getObsoleteSize();
        }

        public void add(Summary o) {
            this.totalSize += o.totalSize;
            this.obsoleteSize += o.obsoleteSize;
        }

        public long totalSize() {
            return this.totalSize;
        }

        public int utilization() {
            return UtilizationProfile.utilization(this.obsoleteSize, this.totalSize);
        }

        @Override
        public String toString() {
            return getAsString();
        }

        public String getAsString() {
            StringBuffer summary = new StringBuffer();

            if(this.fileNum != null)
                summary.append(pad(Long.toHexString(this.fileNum.longValue()), 8, '0'));
            else {
                summary.append(" TOTALS ");
            }
            int kb = (int) (this.totalSize / 1024L);
            summary.append("  ");
            summary.append(pad(Integer.toString(kb), 9, ' '));
            summary.append("     ");
            summary.append(pad(Integer.toString(utilization()), 3, ' '));

            return summary.toString();
        }

        private String pad(String val, int digits, char padChar) {
            StringBuffer result = new StringBuffer();
            int padSize = digits - val.length();
            for(int i = 0; i < padSize; ++i) {
                result.append(padChar);
            }
            result.append(val);

            return result.toString();
        }
    }
}