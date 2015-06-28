package voldemort.store.readonly.fetcher;

import java.io.File;
import java.util.Map;

import voldemort.store.readonly.checksum.CheckSum;
import voldemort.store.readonly.checksum.CheckSum.CheckSumType;

public interface FetchStrategy {
	public Map<HdfsFile, byte[]> fetch(HdfsDirectory directory, File dest)
			throws Throwable;

	public CheckSum fetch(HdfsFile file, File dest, CheckSumType checkSumType)
			throws Throwable;
}
