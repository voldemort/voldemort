package voldemort.store.readonly.fetcher;

import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

import java.io.File;
import java.io.IOException;
import java.util.Map;


public interface FetchStrategy {
    public Map<HdfsFile, byte[]> fetch(HdfsDirectory directory, File dest) throws IOException;

    public CheckSum fetch(HdfsFile file, File dest, CheckSumType checkSumType) throws IOException;
}
