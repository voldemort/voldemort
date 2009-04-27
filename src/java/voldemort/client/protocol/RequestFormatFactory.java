package voldemort.client.protocol;

import voldemort.VoldemortException;
import voldemort.client.protocol.pb.ProtoBuffClientRequestFormat;
import voldemort.client.protocol.vold.VoldemortNativeClientRequestFormat;

/**
 * A factory for producing the appropriate client request format given a
 * {@link voldemort.client.protocol.RequestFormatType}
 * 
 * @author jay
 * 
 */
public class RequestFormatFactory {

    public RequestFormat getRequestFormat(RequestFormatType type) {
        switch(type) {
            case VOLDEMORT:
                return new VoldemortNativeClientRequestFormat();
            case PROTOCOL_BUFFERS:
                return new ProtoBuffClientRequestFormat();
            default:
                throw new VoldemortException("Unknown wire format " + type);
        }
    }

}
