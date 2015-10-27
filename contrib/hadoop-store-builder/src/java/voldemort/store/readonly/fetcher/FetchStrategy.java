package voldemort.store.readonly.fetcher;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import voldemort.store.readonly.checksum.CheckSum.CheckSumType;


public interface FetchStrategy {
    public Map<HdfsFile, byte[]> fetch(HdfsDirectory directory, File dest) throws IOException;

    public byte[] fetch(HdfsFile file, File dest, CheckSumType checkSumType) throws IOException;
}
