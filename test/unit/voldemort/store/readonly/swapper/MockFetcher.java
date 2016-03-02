package voldemort.store.readonly.swapper;

import voldemort.VoldemortException;
import voldemort.server.VoldemortConfig;
import voldemort.server.protocol.admin.AsyncOperationStatus;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.readonly.FileFetcher;
import voldemort.utils.Utils;

import java.io.File;
import java.io.IOException;

/** mock fileFetcher class
 * This class is only for testing purpose.
 * The Fetcher is gonna running for "sleepTime" seconds long.
 */
public class MockFetcher implements FileFetcher {

    static private int sleepTime = 0;

    //This constructor method is necessary since the instance is created by a reflection way
    //which requires VoldemortConfig as an argument.
    public MockFetcher(VoldemortConfig config){
    }

    static public void setSleepTime(int time) { sleepTime = time; }

    @Override
    public File fetch(String source, String dest) throws IOException {
        return new File("mock Fetcher");
    }

    @Override
    public File fetch(String source, String dest, long diskQuotaSizeInKB) throws IOException {
        return this.fetch(source, dest);
    }

    @Override
    public File fetch(String source, String dest, AsyncOperationStatus status, String storeName, long pushVersion, MetadataStore metadataStore, Long diskQuotaSizeInKB) throws IOException, InterruptedException {
        Thread.currentThread().sleep(sleepTime * 1000);
        if (!Utils.isReadableDir(source))
            throw new VoldemortException("Fetch url " + source + " is not readable");

        File destFile = new File(dest);

        if (destFile.exists())
            throw new VoldemortException("Version directory " + destFile.getAbsolutePath()
                    + " already exists");

        Utils.move(new File(source), destFile);
        return destFile;
    }
}
