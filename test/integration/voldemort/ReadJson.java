package voldemort;

import java.io.BufferedReader;
import java.io.FileReader;

import voldemort.serialization.json.JsonReader;
import voldemort.utils.Utils;

/**
 * @author jay
 *
 */
public class ReadJson {

    public static void main(String[] args) throws Exception {
        if(args.length != 1)
            Utils.croak("USAGE: java ReadJson filename");
        long start = System.currentTimeMillis();
        BufferedReader reader = new BufferedReader(new FileReader(args[0]), 1000000);
        JsonReader jReader = new JsonReader(reader);
        int count;
        for(count = 0; jReader.hasMore(); count++) {
            jReader.read();
            if(count % 1000000 == 0)
                System.out.println(count);
        }
        System.out.println((System.currentTimeMillis() - start) / (double) count + " ms/object");
    }
    
}
