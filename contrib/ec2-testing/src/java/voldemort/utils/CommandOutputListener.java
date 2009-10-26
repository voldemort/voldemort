package voldemort.utils;

public interface CommandOutputListener {

    public void outputReceived(String hostName, String line);

}
