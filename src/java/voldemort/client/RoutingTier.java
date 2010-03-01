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

package voldemort.client;

/**
 * Enumerates different routing strategies. Currently just server and client.
 * 
 * 
 */
public enum RoutingTier {
    CLIENT("client"),
    SERVER("server");

    private final String text;

    private RoutingTier(String text) {
        this.text = text;
    }

    public static RoutingTier fromDisplay(String type) {
        for(RoutingTier t: RoutingTier.values())
            if(t.toDisplay().equals(type))
                return t;
        throw new IllegalArgumentException("No RoutingPolicy " + type + " exists.");
    }

    public String toDisplay() {
        return text;
    }
}
