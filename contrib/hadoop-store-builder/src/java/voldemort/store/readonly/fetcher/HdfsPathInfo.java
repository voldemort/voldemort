package voldemort.store.readonly.fetcher;

import java.util.List;

/**
 * This class maintains some metrics about some directories in HDFS
 * which we are interested to fetch.
 *
 * TODO: Determine if we'd rather get rid of this class entirely.
 *       We might want to instead keep just:
 *       {@link voldemort.store.readonly.fetcher.HdfsDirectory}
 */
public class HdfsPathInfo {

    int directories = 0;
    int files = 0;
    long size = 0;

    private HdfsPathInfo() {

    }

    /**
     * This constructor relies on the caller to tell it the list of
     * directories for which it needs to hold statistics. This is
     * useful for the 'build.primary.replicas.only' strategy where
     * we are interested in downloading just a subset of a given
     * directory. That subset is determined higher up the stack.
     *
     * @param directories list of HdfsDirectories to maintain metrics about
     */
    public HdfsPathInfo(List<HdfsDirectory> directories) {
        for (HdfsDirectory directory: directories) {
            this.directories += directory.getNumberOfSubDirectories();
            this.files += directory.getNumberOfFiles();
            this.size += directory.getTotalSizeOfChildren();
        }
    }

    public long getTotalSize() {
        return size;
    }

    @Override
    public String toString() {
        return "directories=" + directories + ", files=" + files + ", size=" + size;
    }

    public static HdfsPathInfo getTestObject(int size) {
        HdfsPathInfo info = new HdfsPathInfo();
        info.size = size;
        return info;
    }
}
