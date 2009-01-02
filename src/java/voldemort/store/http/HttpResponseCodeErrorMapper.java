package voldemort.store.http;

import java.net.HttpURLConnection;

import voldemort.VoldemortException;
import voldemort.store.ObsoleteVersionException;
import voldemort.store.UnknownFailure;

/**
 * A Mapping of HTTP response codes to exceptions
 * 
 * You must add matching entries in both mapResponseCodeToError and
 * mapErrorToResponseCode
 * 
 * @author jay
 * 
 */
public class HttpResponseCodeErrorMapper {

    public VoldemortException mapResponseCodeToError(int responseCode, String message) {
        // 200 range response code is okay!
        if (responseCode >= 200 && responseCode < 300)
            return null;
        else if (responseCode == HttpURLConnection.HTTP_CONFLICT)
            throw new ObsoleteVersionException(message);
        else
            throw new UnknownFailure("Unknown failure occured in HTTP operation: " + responseCode
                                     + " - " + message);
    }

    public ResponseCode mapErrorToResponseCode(VoldemortException v) {
        if (v instanceof ObsoleteVersionException)
            return new ResponseCode(HttpURLConnection.HTTP_CONFLICT, v.getMessage());
        else
            return new ResponseCode(HttpURLConnection.HTTP_BAD_GATEWAY, v.getMessage());
    }

    public void throwError(int responseCode, String message) {
        // 200 range response code is okay!
        if (responseCode >= 200 && responseCode < 300)
            return;
        else
            throw mapResponseCodeToError(responseCode, message);
    }

    /**
     * A struct to hold the response code and response text for an HTTP error.
     * 
     * @author jay
     * 
     */
    public static final class ResponseCode {

        private final int responseCode;
        private final String responseText;

        public ResponseCode(int responseCode, String responseText) {
            super();
            this.responseCode = responseCode;
            this.responseText = responseText;
        }

        public int getCode() {
            return responseCode;
        }

        public String getText() {
            return responseText;
        }

    }

}
