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

package voldemort.server;

/**
 * Thrown when a server is not able to update metadata as requested.
 * 
 * @author bbansal
 * 
 */
public class UnableUpdateMetadataException extends RuntimeException {

    private static final long serialVersionUID = 1;

    public UnableUpdateMetadataException(Exception cause) {
        super("Unable to update Metadata as requested.", cause);
    }

    public UnableUpdateMetadataException(String string) {
        super(string);
    }

    public UnableUpdateMetadataException(String string, Exception exception) {
        super(string, exception);
    }
}
