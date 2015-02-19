package voldemort.store.readonly.hooks.http;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Basic asynchronous http call.
 */
class HttpHookRunnable implements Runnable {

    private String hookName;
    private Logger log;
    private String urlToCall;
    private HttpMethod httpMethod;
    private String contentType;
    private String requestBody;

    public HttpHookRunnable(String hookName,
                            Logger log,
                            String urlToCall,
                            HttpMethod httpMethod,
                            String contentType,
                            String requestBody) {
        this.hookName = hookName;
        this.log = log;
        this.urlToCall = urlToCall;
        this.httpMethod = httpMethod;
        this.contentType = contentType;
        this.requestBody = requestBody;
    }

    @SuppressWarnings("unchecked")
    public void run() {
        HttpURLConnection conn = null;
        OutputStream out = null;
        try {
            URL url = new URL(urlToCall);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(httpMethod.name());
            conn.setDoOutput(true);
            conn.setDoInput(true);
            if (contentType != null)
                conn.setRequestProperty("Content-Type", contentType);

            if (log.isDebugEnabled())
                log.debug("HttpHook [" + hookName + "] will send " + httpMethod.name() + " request to " + urlToCall + " with body: " + requestBody);

            if (requestBody != null) {
                out = conn.getOutputStream();
                out.write(requestBody.getBytes());
            }

            int responseCode = conn.getResponseCode();

            if(responseCode != HttpURLConnection.HTTP_OK) {
                handleResponse(responseCode, conn.getErrorStream());
                throw new IOException("HttpHook [" + hookName + "] received '" +
                        responseCode + ": " + conn.getResponseMessage() +
                        "' response from " + httpMethod + " request to " + urlToCall);
            } else {
                handleResponse(responseCode, conn.getInputStream());
            }
        } catch(Exception e) {
            log.error("Error while sending a request for a HttpHook [" + hookName + "]", e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // no-op
                }
            }

        }
    }

    /**
     * Can be overridden if you want to replace or supplement the debug handling for responses.
     *
     * @param responseCode
     * @param inputStream
     */
    protected void handleResponse(int responseCode, InputStream inputStream) {
        if(log.isDebugEnabled()) {
            BufferedReader rd = null;
            try {
                // Buffer the result into a string
                rd = new BufferedReader(new InputStreamReader(inputStream));
                StringBuilder sb = new StringBuilder();
                String line;
                while((line = rd.readLine()) != null) {
                    sb.append(line);
                }
                log.debug("HttpHook [" + hookName + "] received " + responseCode + " response: " + sb);
            } catch (IOException e) {
                log.debug("Error while reading response for HttpHook [" + hookName + "]", e);
            } finally {
                if (rd != null) {
                    try {
                        rd.close();
                    } catch (IOException e) {
                        // no-op
                    }
                }
            }
        }
    }
}
