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

import java.util.zip.CRC32;

import voldemort.utils.ByteUtils;

public class CRC32CheckSum extends CheckSum {

    private CRC32 checkSumGenerator = null;

    public CRC32CheckSum() {
        checkSumGenerator = new CRC32();
    }

    @Override
    public byte[] getCheckSum() {
        byte[] returnedCheckSum = new byte[ByteUtils.SIZE_OF_LONG];
        ByteUtils.writeLong(returnedCheckSum, checkSumGenerator.getValue(), 0);
        checkSumGenerator.reset();
        return returnedCheckSum;
    }

    @Override
    public void update(byte[] input, int startIndex, int length) {
        checkSumGenerator.update(input, startIndex, length);
    }
}
