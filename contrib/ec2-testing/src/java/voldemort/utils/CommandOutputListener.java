package voldemort.utils;

public interface CommandOutputListener {

    public enum OutputType {
        STDOUT,
        STDERR
    }

    public void outputReceived(OutputType outputType, String hostName, String line);

}
