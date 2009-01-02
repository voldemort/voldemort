package voldemort.partition;

import java.io.*;

import voldemort.utils.FnvHashFunction;

public class FnvHashFunctionTester {

	public static void main(String[] argv) throws Exception {
		if(argv.length  != 1) {
			System.err.println("USAGE: java voldemort.partition.FnvHashFunctionTester filename");
			System.exit(1);
		}

		FnvHashFunction hash = new FnvHashFunction();
		BufferedReader reader = new BufferedReader(new FileReader(argv[0]));
		while(true) {
			String line = reader.readLine();
			if(line == null)
				break;
			System.out.println(hash.hash(line.trim().getBytes()));
		}

		reader.close();

	}

}
