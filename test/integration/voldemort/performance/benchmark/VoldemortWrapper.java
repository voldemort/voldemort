/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.performance.benchmark;

import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.utils.Time;
import voldemort.versioning.Versioned;

public class VoldemortWrapper {

    public enum ReturnCode {
        Ok,
        Error
    }

    private StoreClient<Object, Object, Object> voldemortStore;
    private Metrics measurement;
    private boolean verifyReads;
    private boolean ignoreNulls;

    public enum Operations {
        Read("reads"),
        Delete("deletes"),
        Write("writes"),
        Mixed("transactions");

        private String opString;

        public String getOpString() {
            return this.opString;
        }

        Operations(String opString) {
            this.opString = opString;
        }
    }

    public VoldemortWrapper(StoreClient<Object, Object, Object> storeClient,
                            boolean verifyReads,
                            boolean ignoreNulls) {
        this.voldemortStore = storeClient;
        this.measurement = Metrics.getInstance();
        this.verifyReads = verifyReads;
        this.ignoreNulls = ignoreNulls;
    }

    public void read(Object key, Object expectedValue, Object transforms) {
        long startNs = System.nanoTime();
        Versioned<Object> returnedValue = voldemortStore.get(key, transforms);
        long endNs = System.nanoTime();
        measurement.recordLatency(Operations.Read.getOpString(),
                                  (int) ((endNs - startNs) / Time.NS_PER_MS));

        ReturnCode res = ReturnCode.Ok;
        if(returnedValue == null && !this.ignoreNulls) {
            res = ReturnCode.Error;
        }

        if(verifyReads && !expectedValue.equals(returnedValue.getValue())) {
            res = ReturnCode.Error;
        }

        measurement.recordReturnCode(Operations.Read.getOpString(), res.ordinal());
    }

    public void read(Object key, Object expectedValue) {
        long startNs = System.nanoTime();
        Versioned<Object> returnedValue = voldemortStore.get(key);
        long endNs = System.nanoTime();
        measurement.recordLatency(Operations.Read.getOpString(),
                                  (int) ((endNs - startNs) / Time.NS_PER_MS));

        ReturnCode res = ReturnCode.Ok;
        if(returnedValue == null && !this.ignoreNulls) {
            res = ReturnCode.Error;
        }

        if(verifyReads && !expectedValue.equals(returnedValue.getValue())) {
            res = ReturnCode.Error;
        }

        measurement.recordReturnCode(Operations.Read.getOpString(), res.ordinal());
    }

    public void mixed(final Object key, final Object newValue, final Object transforms) {

        boolean updated = voldemortStore.applyUpdate(new UpdateAction<Object, Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object, Object> storeClient) {
                long startNs = System.nanoTime();
                storeClient.get(key);
                storeClient.put(key, newValue, transforms);
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Mixed.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(updated) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Mixed.getOpString(), res.ordinal());
    }

    public void mixed(final Object key, final Object newValue) {

        boolean updated = voldemortStore.applyUpdate(new UpdateAction<Object, Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object, Object> storeClient) {
                long startNs = System.nanoTime();
                storeClient.get(key);
                storeClient.put(key, newValue);
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Mixed.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(updated) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Mixed.getOpString(), res.ordinal());
    }

    public void write(final Object key, final Object value) {

        boolean written = voldemortStore.applyUpdate(new UpdateAction<Object, Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object, Object> storeClient) {
                long startNs = System.nanoTime();
                storeClient.put(key, value);
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Write.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(written) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Write.getOpString(), res.ordinal());
    }

    public void write(final Object key, final Object value, final Object transforms) {

        boolean written = voldemortStore.applyUpdate(new UpdateAction<Object, Object, Object>() {

            @Override
            public void update(StoreClient<Object, Object, Object> storeClient) {
                long startNs = System.nanoTime();
                storeClient.put(key, value, transforms);
                long endNs = System.nanoTime();
                measurement.recordLatency(Operations.Write.getOpString(),
                                          (int) ((endNs - startNs) / Time.NS_PER_MS));
            }
        });

        ReturnCode res = ReturnCode.Error;
        if(written) {
            res = ReturnCode.Ok;
        }

        measurement.recordReturnCode(Operations.Write.getOpString(), res.ordinal());
    }

    public void delete(Object key) {
        long startNs = System.nanoTime();
        boolean deleted = voldemortStore.delete(key);
        long endNs = System.nanoTime();

        ReturnCode res = ReturnCode.Error;
        if(deleted) {
            res = ReturnCode.Ok;
        }

        measurement.recordLatency(Operations.Delete.getOpString(),
                                  (int) ((endNs - startNs) / Time.NS_PER_MS));
        measurement.recordReturnCode(Operations.Delete.getOpString(), res.ordinal());
    }
}
