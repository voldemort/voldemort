package voldemort.performance;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.utils.CmdUtils;
import voldemort.utils.PseudoRandom;

import java.util.List;

/**
 * @author afeinber
 * Generate data pseudo-random data with desired parameters on a remote cluster
 */
public class RemoteDataGenerator {
    private final PseudoRandom prng = new PseudoRandom();
    private final String url;
    private final String storeName;
    private final int workers;
    private static final int MAX_WORKERS = 8;

    public RemoteDataGenerator(String url, String storeName, int workers) {
        this.workers = workers;
        this.url = url;
        this.storeName = storeName;
    }

    /**
     * Populate the store with {@param requests} key-value pairs with key of size,
     * appending {@param postfix} for uniqueness.
     *
     * {@param keySize} bytes and value of size {@param valueSize} bytes
     * @param requests How many key-value pairs to generate
     * @param keySize Size (in bytes) of the key
     * @param valueSize Size (in bytes) of the value
     * @param postfix Postfix to append (for uniqueness)
     */
    public void
    generateData(int requests, int keySize, int valueSize, String postfix) {
        StoreClientFactory storeClientFactory = new
                SocketStoreClientFactory(new ClientConfig()
                .setBootstrapUrls(url)
                .setMaxThreads(workers));
        StoreClient<String,String> client = storeClientFactory.getStoreClient(storeName);

        for (int i=0; i < requests; i++) {
            StringBuilder keyBuilder = new StringBuilder(makeString(keySize))
                    .append(i);
            StringBuilder valueBuilder = new StringBuilder(makeString(valueSize))
                    .append(i);
            if (postfix != null) {
                valueBuilder.append(postfix);
                keyBuilder.append(postfix);
            }
            try {
                client.put(keyBuilder.toString(), valueBuilder.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * Pads a string with random longs until it reaches {@param size} bytes
     * @param size Desired bytes in the string
     * @return String of length >= {@param size} composed of pseudo-random longs
     */
    protected String makeString(int size) {
        StringBuilder output = new StringBuilder();

        // Java Strings are two bytes per character
        while(output.length() < size*2) {
            output.append(prng.random());
        }

        return output.toString();
    }

    public static void main (String[] args) {
        OptionParser parser = new OptionParser();
        parser.accepts("k", "key size")
                .withRequiredArg()
                .ofType(Integer.class);
        parser.accepts("v", "value size")
                .withRequiredArg()
                .ofType(Integer.class);
        parser.accepts("p", "prefix")
                .withRequiredArg();

        OptionSet options = parser.parse(args);
        List<String> nonOptions = options.nonOptionArguments();
        if (nonOptions.size() != 3) {

        }
        String url = nonOptions.get(0);
        String storeName = nonOptions.get(1);

        int requests = Integer.parseInt(nonOptions.get(2));
        int keySize = CmdUtils.valueOf(options, "k", 128);
        int valueSize = CmdUtils.valueOf(options, "v", 256);
        int workers = CmdUtils.valueOf(options, "threads", MAX_WORKERS);
        String postfix = (String) (options.has("p") ?
                options.valueOf("p") :
                null);
        RemoteDataGenerator rdg = new RemoteDataGenerator(url, storeName, workers);
        rdg.generateData(requests, keySize, valueSize, postfix);
    }
}
