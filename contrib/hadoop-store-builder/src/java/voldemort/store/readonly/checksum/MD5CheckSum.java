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

package voldemort.store.readonly.checksum;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5CheckSum extends CheckSum {

    private MessageDigest checkSumGenerator = null;

    public MD5CheckSum() throws NoSuchAlgorithmException {
        checkSumGenerator = MessageDigest.getInstance("md5");
    }

    @Override
    public byte[] getCheckSum() {
        return checkSumGenerator.digest();
    }

    @Override
    public void update(byte[] input, int startIndex, int length) {
        checkSumGenerator.update(input, startIndex, length);
    }

    @Override
    public void reset() {
        checkSumGenerator.reset();
    }

}
