package voldemort.utils;


public class ConfigurationException extends RuntimeException {

	final static long serialVersionUID = 1;

	public ConfigurationException(String message){
		super(message);
	}

	public ConfigurationException(Exception cause){
		super(cause);
	}

}
