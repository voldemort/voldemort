/*
 * Copyright 2014 LinkedIn, Inc
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
package voldemort.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class OpTimeMapTest {

    @Test
    public void testMapsEqualAfterCopyConstruction() {
        final OpTimeMap sourceMap = createSourceMap();
        final OpTimeMap destMap = new OpTimeMap(sourceMap);

        assertEquals(sourceMap, destMap);
    }

    @Test
    public void testOpsEqualAfterCopyConstruction() {
        final OpTimeMap sourceMap = createSourceMap();
        final OpTimeMap destMap = new OpTimeMap(sourceMap);

        assertEquals(sourceMap.getOpTime(VoldemortOpCode.DELETE_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.DELETE_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.DELETE_PARTITIONS_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.DELETE_PARTITIONS_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.GET_ALL_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.GET_ALL_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.GET_METADATA_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.GET_METADATA_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.GET_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.GET_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.GET_VERSION_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.GET_VERSION_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.PUT_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.PUT_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.REDIRECT_GET_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.REDIRECT_GET_OP_CODE));
        assertEquals(sourceMap.getOpTime(VoldemortOpCode.UPDATE_METADATA_OP_CODE),
                     destMap.getOpTime(VoldemortOpCode.UPDATE_METADATA_OP_CODE));
    }

    @Test
    public void testMapsNotEqualAfterCopyConstructionAndOpChange() {
        final OpTimeMap sourceMap = createSourceMap();
        final OpTimeMap destMap = new OpTimeMap(sourceMap);
        sourceMap.setOpTime(VoldemortOpCode.GET_OP_CODE, 0L);

        assertFalse(sourceMap.equals(destMap));
    }

    private static OpTimeMap createSourceMap() {
        final OpTimeMap sourceMap = new OpTimeMap(5L);
        sourceMap.setOpTime(VoldemortOpCode.DELETE_OP_CODE, 1L);
        sourceMap.setOpTime(VoldemortOpCode.DELETE_PARTITIONS_OP_CODE, 2L);
        sourceMap.setOpTime(VoldemortOpCode.GET_ALL_OP_CODE, 3L);
        sourceMap.setOpTime(VoldemortOpCode.GET_METADATA_OP_CODE, 4L);
        sourceMap.setOpTime(VoldemortOpCode.GET_OP_CODE, 5L);
        sourceMap.setOpTime(VoldemortOpCode.GET_PARTITION_AS_STREAM_OP_CODE, 6L);
        sourceMap.setOpTime(VoldemortOpCode.GET_VERSION_OP_CODE, 7L);
        sourceMap.setOpTime(VoldemortOpCode.PUT_ENTRIES_AS_STREAM_OP_CODE, 8L);
        sourceMap.setOpTime(VoldemortOpCode.PUT_OP_CODE, 9L);
        sourceMap.setOpTime(VoldemortOpCode.REDIRECT_GET_OP_CODE, 10L);
        sourceMap.setOpTime(VoldemortOpCode.UPDATE_METADATA_OP_CODE, 11L);
        return sourceMap;
    }

}
