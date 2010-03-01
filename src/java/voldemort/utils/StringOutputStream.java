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

package voldemort.utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A simple string Output Stream.
 * 
 * 
 */
public class StringOutputStream extends OutputStream {

    StringBuilder mBuf = new StringBuilder();

    @Override
    public void write(int data) throws IOException {
        mBuf.append((char) data);
    }

    public String getString() {
        return mBuf.toString();
    }
}
