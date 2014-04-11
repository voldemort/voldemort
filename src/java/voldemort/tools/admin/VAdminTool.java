/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools.admin;

/**
 * Provides a command line interface to the
 * {@link voldemort.client.protocol.admin.AdminClient}
 * 
 */
public class VAdminTool {
    public static void main(String[] args) {
        try {
        	AdminCommand.executeCommand(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
