/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.cluster.failuredetector;

import voldemort.cluster.Node;

/**
 * FailureDetectorListener is a simple means by which interested parties can
 * listen for changes to the availability of nodes.
 * 
 * <p/>
 * 
 * <b>Note for implementors</b>: Make sure that the FailureDetectorListener
 * implementation properly implements the hashCode/equals methods to ensure
 * adding and removing instances from the FailureDetector work as expected.
 * 
 * 
 * @see FailureDetector#addFailureDetectorListener
 * @see FailureDetector#removeFailureDetectorListener
 */

public interface FailureDetectorListener {

    public void nodeUnavailable(Node node);

    public void nodeAvailable(Node node);

}
