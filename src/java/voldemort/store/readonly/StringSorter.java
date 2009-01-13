package voldemort.store.readonly;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import org.apache.commons.io.LineIterator;

import voldemort.serialization.StringSerializer;
import voldemort.utils.Utils;

/**
 * Perform an external sort on the lines of the file
 * 
 * @author jay
 * 
 */
public class StringSorter {

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java StringSorter inputfile internal_sort_size");
        String input = args[0];
        int internalSortSize = Integer.parseInt(args[1]);

        ExternalSorter<String> sorter = new ExternalSorter<String>(new StringSerializer(),
                                                                   internalSortSize);
        LineIterator it = new LineIterator(new BufferedReader(new FileReader(input),
                                                              10 * 1024 * 1024));

        String seperator = System.getProperty("line.separator");
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out),
                                                   10 * 1024 * 1024);
        for(String line: sorter.sorted(it)) {
            writer.write(line);
            writer.write(seperator);
        }
    }

}
