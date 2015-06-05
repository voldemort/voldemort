package voldemort.store.readonly.fetcher;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HdfsPathInfo {

    int directories = 0;
    int files = 0;
    long size = 0;

    private HdfsPathInfo() {

    }

    public HdfsPathInfo(FileSystem fs, Path path) throws IOException {
        addDirectory(fs, path);
    }

    private void addSize(long newSize) {
        this.files++;
        this.size += newSize;
    }

    private void addDirectory(FileSystem fs, Path path) throws IOException {
        directories++;

        FileStatus[] statuses = fs.listStatus(path);
        if(statuses != null) {
            for(FileStatus status: statuses) {
                if(status.isDir()) {
                    addDirectory(fs, status.getPath());
                } else {
                    addSize(status.getLen());
                }
            }
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
