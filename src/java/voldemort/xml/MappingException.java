package voldemort.xml;

import voldemort.VoldemortException;

public class MappingException extends VoldemortException {

    public static final long serialVersionUID = 1L;

    public MappingException() {
        super();
    }

    public MappingException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public MappingException(String arg0) {
        super(arg0);
    }

    public MappingException(Throwable arg0) {
        super(arg0);
    }

}
