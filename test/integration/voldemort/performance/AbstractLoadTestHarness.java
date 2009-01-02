package voldemort.performance;

import static voldemort.utils.Utils.croak;

import java.io.File;

import voldemort.client.StoreClient;
import voldemort.utils.Props;
import voldemort.versioning.Versioned;

public abstract class AbstractLoadTestHarness {

	public abstract StoreClient<String,String> getStore(Props propsA, Props propsB) throws Exception;
	
	public void run(String[] args) throws Exception {
		if(args.length != 4)
			croak("USAGE: java " + RemoteTest.class.getName() +
					" numRequests numThreads properties_file1 properties_file2");

		int numRequests = Integer.parseInt(args[0]);
		int numThreads = Integer.parseInt(args[1]);
		Props propertiesA = new Props(new File(args[2]));
		Props propertiesB = new Props(new File(args[3]));
		
		System.out.println("Creating client: ");
		final StoreClient<String,String> client = getStore(propertiesA, propertiesB);
		
		PerformanceTest writeTest = new PerformanceTest() {
            public void doOperation(int index) throws Exception {
                String iStr = Integer.toString(index);
                client.put(iStr, new Versioned<String>(iStr));
            }
		};
		System.out.println();
		System.out.println("WRITE TEST");
		writeTest.run(numRequests, numThreads);
		writeTest.printStats();

		System.out.println();
		
        PerformanceTest readTest = new PerformanceTest() {
            public void doOperation(int index) throws Exception {
                String iStr = Integer.toString(index);
                String found = client.getValue(iStr);
                if(!iStr.equals(found))
                    System.err.println("Not equal: " + iStr + ", " + found);
             }
        };
    
        System.out.println("READ TEST");
        readTest.run(numRequests, numThreads);
        readTest.printStats();

		System.exit(0);
	}
	
}
