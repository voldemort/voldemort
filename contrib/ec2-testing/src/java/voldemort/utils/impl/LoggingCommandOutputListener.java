/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils.impl;

import org.apache.commons.logging.Log;

public class LoggingCommandOutputListener extends DelegatingCommandOutputListener {

    private final Log logger;

    private final boolean shouldProcessExceptions;

    public LoggingCommandOutputListener(CommandOutputListener delegate,
                                        Log logger,
                                        boolean shouldProcessExceptions) {
        super(delegate);
        this.logger = logger;
        this.shouldProcessExceptions = shouldProcessExceptions;
    }

    @Override
    public void outputReceived(String hostName, String line) {
        if(shouldProcessExceptions && (line.contains("Exception") || line.startsWith("\tat"))) {
            if(logger.isWarnEnabled())
                logger.warn(hostName + ": " + line);
        } else {
            if(logger.isDebugEnabled())
                logger.debug(hostName + ": " + line);
        }

        super.outputReceived(hostName, line);
    }

}
