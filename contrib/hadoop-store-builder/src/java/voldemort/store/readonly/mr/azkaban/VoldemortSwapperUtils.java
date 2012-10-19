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

package voldemort.store.readonly.mr.azkaban;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import voldemort.cluster.Node;

@Deprecated
public class VoldemortSwapperUtils {

    public static void doSwap(String storeName, Node node, String destinationDir)
            throws IOException {
        // construct data operation = swap
        String data = URLEncoder.encode("operation", "UTF-8") + "="
                      + URLEncoder.encode("swap", "UTF-8");
        // add index = indexFileName
        data += "&" + URLEncoder.encode("index", "UTF-8") + "="
                + URLEncoder.encode(getIndexDestinationFile(node.getId(), destinationDir), "UTF-8");
        // add data = dataFileName
        data += "&" + URLEncoder.encode("data", "UTF-8") + "="
                + URLEncoder.encode(getDataDestinationFile(node.getId(), destinationDir), "UTF-8");
        // add store= storeName
        data += "&" + URLEncoder.encode("store", "UTF-8") + "="
                + URLEncoder.encode(storeName, "UTF-8");

        // Send data
        URL url = new URL("http://" + node.getHost() + ":" + node.getHttpPort() + "/read-only/mgmt");
        System.out.println("swapping node:" + node.getId() + " with url:" + url.toString()
                           + " data:" + data);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

        connection.setRequestProperty("Content-Length",
                                      "" + Integer.toString(data.getBytes().length));
        connection.setRequestProperty("Content-Language", "en-US");

        connection.setUseCaches(false);
        connection.setDoInput(true);
        connection.setDoOutput(true);

        OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());
        wr.write(data);
        wr.flush();
        wr.close();
        // Get Response
        InputStream is = connection.getInputStream();
        BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        String line;
        StringBuffer response = new StringBuffer();
        while((line = rd.readLine()) != null) {
            response.append(line);
            response.append('\r');
        }
        System.out.println("doSwap Completed for " + node + "  Response:" + response.toString());
        rd.close();
    }

    public static String getIndexDestinationFile(int nodeId, String destinationDir) {
        return destinationDir + "/" + "node-" + nodeId + ".index";
    }

    public static String getDataDestinationFile(int nodeId, String destinationDir) {
        return destinationDir + "/" + "node-" + nodeId + ".data";
    }
}
