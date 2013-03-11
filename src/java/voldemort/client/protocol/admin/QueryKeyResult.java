/*
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.client.protocol.admin;

import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

/**
 * Return type of AdminClient.QueryKeys. Intended to ensure the following
 * invariant: .hasValues() == !.hasException()
 */
public class QueryKeyResult {

    private final ByteArray key;
    private final List<Versioned<byte[]>> values;
    private final Exception exception;

    public QueryKeyResult(ByteArray key, List<Versioned<byte[]>> values) {
        this.key = key;
        this.values = values;
        this.exception = null;
    }

    public QueryKeyResult(ByteArray key, Exception exception) {
        this.key = key;
        this.values = null;
        this.exception = exception;
    }

    public ByteArray getKey() {
        return key;
    }

    /**
     * @return true iff values were returned.
     */
    public boolean hasValues() {
        return (values != null);
    }

    public List<Versioned<byte[]>> getValues() {
        return values;
    }

    /**
     * @return true iff exception occured during queryKeys.
     */
    public boolean hasException() {
        return (exception != null);
    }

    public Exception getException() {
        return exception;
    }
}