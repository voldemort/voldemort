/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.store.http;

import java.net.HttpURLConnection;

import voldemort.VoldemortException;
import voldemort.store.UnknownFailure;
import voldemort.versioning.ObsoleteVersionException;

/**
 * A Mapping of HTTP response codes to exceptions
 * 
 * You must add matching entries in both mapResponseCodeToError and
 * mapErrorToResponseCode
 * 
 * 
 */
public class HttpResponseCodeErrorMapper {

    public VoldemortException mapResponseCodeToError(int responseCode, String message) {
        // 200 range response code is okay!
        if(responseCode >= 200 && responseCode < 300)
            return null;
        else if(responseCode == HttpURLConnection.HTTP_CONFLICT)
            throw new ObsoleteVersionException(message);
        else
            throw new UnknownFailure("Unknown failure occured in HTTP operation: " + responseCode
                                     + " - " + message);
    }

    public ResponseCode mapErrorToResponseCode(VoldemortException v) {
        if(v instanceof ObsoleteVersionException)
            return new ResponseCode(HttpURLConnection.HTTP_CONFLICT, v.getMessage());
        else
            return new ResponseCode(HttpURLConnection.HTTP_BAD_GATEWAY, v.getMessage());
    }

    public void throwError(int responseCode, String message) {
        // 200 range response code is okay!
        if(responseCode >= 200 && responseCode < 300)
            return;
        else
            throw mapResponseCodeToError(responseCode, message);
    }

    /**
     * A struct to hold the response code and response text for an HTTP error.
     * 
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
