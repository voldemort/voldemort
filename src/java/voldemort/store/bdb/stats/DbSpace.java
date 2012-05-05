package voldemort.store.bdb.stats;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.cleaner.UtilizationProfile;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.UtilizationFileReader;

/**
 * Code based in the com.sleepycat.je.util.DbSpace class.
 */
public class DbSpace {

    private final boolean recalc;
    private final EnvironmentImpl envImpl;
    private Summary totals = new Summary();
    private Summary[] summaries = null;
    private StringBuffer summaryDetails = new StringBuffer();

    public DbSpace(Environment env) {
        this(DbInternal.getEnvironmentImpl(env), false);
    }

    public DbSpace(Environment env, boolean recalc) {
        this(DbInternal.getEnvironmentImpl(env), recalc);
    }

    private DbSpace(EnvironmentImpl envImpl, boolean recalc) {
        this.envImpl = envImpl;
        this.recalc = recalc;

        UtilizationProfile profile = this.envImpl.getUtilizationProfile();
        SortedMap<Long, FileSummary> map = profile.getFileSummaryMap(true);
        Map<Long, FileSummary> recalcMap = (this.recalc) ? UtilizationFileReader.calcFileSummaryMap(this.envImpl)
                                                        : null;

        int fileIndex = 0;

        totals = new Summary();
        summaries = new Summary[map.size()];

        Iterator<Map.Entry<Long, FileSummary>> iter = map.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<Long, FileSummary> entry = iter.next();
            Long fileNum = entry.getKey();
            FileSummary fs = entry.getValue();
            FileSummary recalcFs = null;
            if(recalcMap != null) {
                recalcFs = recalcMap.get(fileNum);
            }

            Summary summary = new Summary(fileNum, fs, recalcFs);
            summaries[fileIndex] = summary;

            totals.add(summary);
            fileIndex++;
        }

        summaryDetails.append((this.recalc) ? "  File    Size (KB)  % Used  % Used (recalculated)\n--------  ---------  ------  ------\n"
                                           : "  File    Size (KB)  % Used\n--------  ---------  ------\n");
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
        private long recalcObsoleteSize;
        private FileSummary fileSummary = null;
        private FileSummary recalcSummary = null;

        public Summary() {}

        public Summary(Long fileNum, FileSummary summary, FileSummary recalcSummary) {
            this.fileNum = fileNum;
            this.fileSummary = summary;
            this.totalSize = summary.totalSize;
            this.obsoleteSize = summary.getObsoleteSize();
            if(recalcSummary != null) {
                this.recalcSummary = recalcSummary;
                this.recalcObsoleteSize = recalcSummary.getObsoleteSize();
            }
        }

        public void add(Summary o) {
            this.totalSize += o.totalSize;
            this.obsoleteSize += o.obsoleteSize;
            this.recalcObsoleteSize += o.recalcObsoleteSize;
        }

        public long totalSize() {
            return this.totalSize;
        }

        public int utilization() {
            return UtilizationProfile.utilization(this.obsoleteSize, this.totalSize);
        }

        public int recalcUtilization() {
            return UtilizationProfile.utilization(this.recalcObsoleteSize, this.totalSize);
        }

        @Override
        public String toString() {
            return getAsString(false);
        }

        public String getAsString(boolean details) {
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
            if(recalcSummary != null) {
                summary.append("     ");
                summary.append(pad(Integer.toString(recalcUtilization()), 3, ' '));
            }
            if(details) {
                summary.append("     ");
                summary.append(fileSummary);

                if(recalcSummary != null) {
                    summary.append("     ");
                    summary.append(recalcSummary);
                }
            }
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