package voldemort.server.protocol.admin;

import voldemort.VoldemortApplicationException;

/**
 * This exception is thrown when fetch is disabled explicitly on the voldemort
 * server you can see the state of the fetcher on a server by running the
 * command
 * 
 * bin/vadmin.sh meta get readonly.fetch.enabled --url <AdminUrl>
 */
public class ReadOnlyFetchDisabledException extends VoldemortApplicationException {

    public ReadOnlyFetchDisabledException(String s) {
        super(s);
    }
}
