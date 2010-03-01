package voldemort.utils;

import java.io.File;
import java.util.Arrays;
import java.util.Stack;

import voldemort.annotations.concurrency.NotThreadsafe;

import com.google.common.collect.AbstractIterator;

/**
 * An iterator over all the files contained in a set of directories, including
 * any subdirectories
 * 
 * 
 */
@NotThreadsafe
public class DirectoryIterator extends AbstractIterator<File> implements ClosableIterator<File> {

    private final Stack<File> stack;

    public DirectoryIterator(String... basis) {
        this.stack = new Stack<File>();
        for(String f: basis)
            stack.add(new File(f));
    }

    public DirectoryIterator(File... basis) {
        this.stack = new Stack<File>();
        stack.addAll(Arrays.asList(basis));
    }

    @Override
    protected File computeNext() {
        while(stack.size() > 0) {
            File f = stack.pop();
            if(f.isDirectory()) {
                for(File sub: f.listFiles())
                    stack.push(sub);
            } else {
                return f;
            }
        }
        return endOfData();
    }

    public void close() {}

    /**
     * Command line method to walk the directories provided on the command line
     * and print out their contents
     * 
     * @param args Directory names
     */
    public static void main(String[] args) {
        DirectoryIterator iter = new DirectoryIterator(args);
        while(iter.hasNext())
            System.out.println(iter.next().getAbsolutePath());
    }

}
