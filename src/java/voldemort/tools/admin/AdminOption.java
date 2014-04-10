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

import joptsimple.OptionParser;
import joptsimple.OptionSpecBuilder;

/**
 * The class defined for all command-line options.
 * 
 */
public class AdminOption<T> {
    public String optName, optDesc, argDesc;
    public char argDelim;
    public Class<T> argType;
    
    /**
     * Initializes an AdminOption which has no argument.
     * 
     * @param _optName Name of option
     * @param _optDesc Description of option
     * @param _argType Class type of argument
     */
    public AdminOption(String optName, String optDesc, Class<T> argType) {
    	this(optName, optDesc, "", argType);
    }

    /**
     * Initializes an AdminOption which has one argument.
     * 
     * @param _optName Name of option
     * @param _optDesc Description of option
     * @param _argType Class type of argument
     * @param _argDesc Description of argument
     */
    public AdminOption(String optName, String optDesc, String argDesc, Class<T> argType) {
        this(optName, optDesc, argDesc, '\0', argType);
    }

    /**
     * Initializes an AdminOption which has multiple argument.
     * 
     * @param _optName Name of option
     * @param _optDesc Description of option
     * @param _argType Class type of argument
     * @param _argDesc Description of argument
     * @param _argDelim Delimiter for multiple arguments
     */
    public AdminOption(String optName, String optDesc, String argDesc, char argDelim, Class<T> argType) {
        this.optName = optName;
        this.optDesc = optDesc;
        this.argDesc = argDesc;
        this.argDelim = argDelim;
        this.argType = argType;
    }
    
    /**
     * Let AdminOption be accepted by JOpt Simple parser.
     *
     * @param parser JOpt Simple parser
     */
    public void join(OptionParser parser) {
        OptionSpecBuilder builder = parser.accepts(optName, optDesc);
        if (argDesc.compareTo("") != 0) {
            if (argDelim != '\0') {
                builder.withRequiredArg()
                       .describedAs(argDesc)
                       .withValuesSeparatedBy(argDelim)
                       .ofType(argType);
            } else {
                builder.withRequiredArg()
                       .describedAs(argDesc)
                       .ofType(argType);
            }
        }
    }
}
