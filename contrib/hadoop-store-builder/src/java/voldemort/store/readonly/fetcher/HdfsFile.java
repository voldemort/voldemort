package voldemort.store.readonly.fetcher;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class HdfsFile implements Comparable<HdfsFile> {

    public enum FileType {
        METADATA,
        OTHER,
        DATA,
        INDEX
    }

    private final FileStatus fs;
    private final FileType type;
    
    private FileType getFileType(String fileName) {
        if(fileName.contains(HdfsFetcher.METADATA_FILE_EXTENSION)) {
            return FileType.METADATA;
        } else if(fileName.contains(HdfsFetcher.DATA_FILE_EXTENSION)) {
            return FileType.DATA;
        } else if(fileName.contains(HdfsFetcher.INDEX_FILE_EXTENSION)) {
            return FileType.INDEX;
        } else {
            return FileType.OTHER;
        }
    }

    public HdfsFile(FileStatus fs) {
        this.fs = fs;
        this.type = getFileType(fs.getPath().getName());
    }

    public Path getPath() {
        return fs.getPath();
    }

    public FileType getFileType() {
        return type;
    }

    public long getSize() {
        if(fs.isDir())
            return -1;
        return fs.getLen();
    }

    @Override
    public boolean equals(Object other) {
        if(this == other) {
            return true;
        }
        if(other instanceof HdfsFile) {
            return ((HdfsFile) other).fs.equals(this.fs);
        }
        return false;
    }
    @Override
    public int hashCode() {
        return fs.hashCode();
    }

    @Override
    public int compareTo(HdfsFile other) {
        FileStatus fs1 = this.fs;
        FileStatus fs2 = other.fs;

        // This is probably dead code, no directories should be present.
        // directories before files
        if(fs1.isDir())
            return fs2.isDir() ? 0 : -1;
        if(fs2.isDir())
            return fs1.isDir() ? 0 : 1;


        // if both are of same type, sort lexicographically
        if(this.type == other.type) {
            String f1 = fs1.getPath().getName(), f2 = fs2.getPath().getName();
            return f1.compareToIgnoreCase(f2);
        }

        // Move the index files to the end, this is a heuristic as they might be
        // available in the page cache
        if(this.type == FileType.INDEX) {
            return 1;
        } else {
            return -1;
        }
    }

    public String getDiskFileName() {
        String fileName = getPath().getName();
        // DiskFile is uncompressed
        if(fileName.endsWith(HdfsFetcher.GZIP_FILE_EXTENSION)) {
            fileName = fileName.substring(0,
                                      fileName.length() - HdfsFetcher.GZIP_FILE_EXTENSION.length());
        }
        return fileName;
    }

    public boolean isCompressed() {
        return getPath().getName().endsWith(HdfsFetcher.GZIP_FILE_EXTENSION);
    }

}
