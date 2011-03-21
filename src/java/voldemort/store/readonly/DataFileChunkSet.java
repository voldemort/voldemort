package voldemort.store.readonly;

import java.nio.channels.FileChannel;
import java.util.List;

public class DataFileChunkSet {

    private final List<FileChannel> dataFiles;
    private final List<Integer> dataFileSizes;

    public DataFileChunkSet(List<FileChannel> dataFiles, List<Integer> dataFileSizes) {
        this.dataFiles = dataFiles;
        this.dataFileSizes = dataFileSizes;
    }

    public int getDataFileSize(int chunk) {
        return this.dataFileSizes.get(chunk);
    }

    public FileChannel dataFileFor(int chunk) {
        return dataFiles.get(chunk);
    }

    public int getNumChunks() {
        return dataFiles.size();
    }
}
