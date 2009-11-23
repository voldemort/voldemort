package voldemort.server.protocol;

import voldemort.client.protocol.RequestFormatType;

/**
 * A factory that gets the appropriate request handler for a given
 * {@link voldemort.client.RequestFormatType}.
 * 
 * @author jay
 * 
 */
public interface RequestHandlerFactory {

    public RequestHandler getRequestHandler(RequestFormatType type);
}
