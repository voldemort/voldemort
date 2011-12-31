/*
 * 
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

package voldemort.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;

public class VoldemortIOUtils {

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 4;

    private static final Logger logger = Logger.getLogger(VoldemortIOUtils.class);

    public static String toString(final InputStream input, final long limit) throws IOException {
        return toString(input, null, limit);
    }

    public static void closeQuietly(final HttpResponse httpResponse, final String context) {
        if(httpResponse != null && httpResponse.getEntity() != null) {
            try {
                IOUtils.closeQuietly(httpResponse.getEntity().getContent());
            } catch(Exception e) {
                logger.error("Error closing entity connection : " + context, e);
            }
        }
    }

    public static void closeQuietly(final HttpResponse httpResponse) {
        closeQuietly(httpResponse, "");
    }

    public static void closeQuietly(HttpClient httpClient) {
        if(httpClient != null) {
            httpClient.getConnectionManager().shutdown();
        }
    }

    public static String toString(final InputStream input, final String encoding, final long limit)
            throws IOException {
        StringWriter sw = new StringWriter();
        copy(input, sw, encoding, limit);
        return sw.toString();
    }

    public static void copy(InputStream input, Writer output, String encoding, final long limit)
            throws IOException {
        if(encoding == null) {
            copy(input, output, limit);
        } else {
            InputStreamReader in = new InputStreamReader(input, encoding);
            copy(in, output, limit);
        }
    }

    public static void copy(InputStream input, Writer output, final long limit) throws IOException {
        InputStreamReader in = new InputStreamReader(input);
        copy(in, output, limit);
    }

    public static int copy(Reader input, Writer output, long limit) throws IOException {
        long count = copyLarge(input, output, limit);
        if(count > Integer.MAX_VALUE) {
            return -1;
        }
        return (int) count;
    }

    public static long copyLarge(Reader input, Writer output, long limit) throws IOException {
        char[] buffer = new char[DEFAULT_BUFFER_SIZE];
        long count = 0;
        int n = 0;
        long remaining = limit;
        while(remaining > 0) {
            n = (remaining > DEFAULT_BUFFER_SIZE) ? input.read(buffer)
                                                 : input.read(buffer, 0, (int) remaining);
            if(n == -1) {
                break;
            }
            output.write(buffer, 0, n);
            count += n;
            remaining -= n;
        }
        return count;
    }
}
