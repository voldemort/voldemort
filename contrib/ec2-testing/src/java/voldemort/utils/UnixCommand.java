package voldemort.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * A wrapper for executing a unix command
 * 
 * @author jay
 * 
 */
public class UnixCommand {

    private final String[] args;

    public static void main(String[] args) throws Exception {
        UnixCommand cmd = new UnixCommand(args);
        cmd.execute();
    }

    public UnixCommand(String... args) {
        this.args = args;
    }

    public UnixCommand(List<String> args) {
        this.args = args.toArray(new String[args.size()]);
    }

    public void execute() throws InterruptedException, IOException {
        ProcessBuilder builder = new ProcessBuilder(this.args);
        Process process = builder.start();

        Thread stdout = new StreamPrinter(process.getInputStream());
        Thread stderr = new StreamPrinter(process.getErrorStream());
        stdout.start();
        stderr.start();

        process.waitFor();

        int exitCode = process.exitValue();

        if(exitCode > 0)
            throw new RuntimeException(exitCode + "");
    }

    public String getCommand() {
        return StringUtils.join(args, ' ');
    }

    @Override
    public String toString() {
        return StringUtils.join(args, ' ');
    }

    private static class StreamPrinter extends Thread {

        private BufferedReader reader;

        public StreamPrinter(InputStream inputStream) {
            this.reader = new BufferedReader(new InputStreamReader(inputStream));
        }

        @Override
        public void run() {
            try {
                String line = null;

                while((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }

}
