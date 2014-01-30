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

package voldemort.store.quota;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortApplicationException;
import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.configuration.FileBackedCachingStorageEngine;
import voldemort.store.stats.StoreStats;
import voldemort.store.stats.Tracked;
import voldemort.utils.ByteArray;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class QuotaLimitingStore extends DelegatingStore<ByteArray, byte[], byte[]> {

    private static final Logger logger = Logger.getLogger(QuotaLimitingStore.class.getName());

    private final StoreStats storeStats;
    private final QuotaLimitStats quotaStats;
    private final FileBackedCachingStorageEngine quotaStore;

    private final String getQuotaKey;
    private final String putQuotaKey;
    private final String getAllQuotaKey;
    private final String deleteQuotaKey;

    public QuotaLimitingStore(Store<ByteArray, byte[], byte[]> innerStore,
                              StoreStats storeStats,
                              QuotaLimitStats quotaStats,
                              FileBackedCachingStorageEngine quotaStore) {
        super(innerStore);
        this.storeStats = storeStats;
        this.quotaStore = quotaStore;

        this.getQuotaKey = QuotaUtils.makeQuotaKey(innerStore.getName(), QuotaType.GET_THROUGHPUT);
        this.putQuotaKey = QuotaUtils.makeQuotaKey(innerStore.getName(), QuotaType.PUT_THROUGHPUT);
        this.getAllQuotaKey = QuotaUtils.makeQuotaKey(innerStore.getName(),
                                                      QuotaType.GET_ALL_THROUGHPUT);
        this.deleteQuotaKey = QuotaUtils.makeQuotaKey(innerStore.getName(),
                                                      QuotaType.DELETE_THROUGHPUT);
        this.quotaStats = quotaStats;
    }

    /**
     * Ensure the current throughput levels for the tracked operation does not
     * exceed set quota limits. Throws an exception if exceeded quota.
     * 
     * @param quotaKey
     * @param trackedOp
     */
    private void checkRateLimit(String quotaKey, Tracked trackedOp) {
        String quotaValue = null;
        try {
            quotaValue = quotaStore.cacheGet(quotaKey);
            // Store may not have any quotas
            if(quotaValue == null) {
                return;
            }
            // But, if it does
            float currentRate = this.storeStats.getThroughput(trackedOp);
            float allowedRate = Float.parseFloat(quotaValue);
            // FIXME VC the histogram should be reasonably accurate to do all
            // these things..
            if(currentRate > allowedRate) {
                quotaStats.reportRateLimitedOp(trackedOp);
                // TODO should be throwing a whole new class of Exception here.
                // But, then it warrants a client code dependency to be able to
                // recognize the exception.So may be later.
                throw new VoldemortApplicationException("Exceeded rate limit for " + quotaKey
                                                        + ". Maximum allowed : " + allowedRate
                                                        + " Current: " + currentRate);
            }
        } catch(NumberFormatException nfe) {
            // move on, if we cannot parse quota value properly
            logger.debug("Invalid formatting of quota value for key " + quotaKey + " : "
                         + quotaValue);
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        checkRateLimit(deleteQuotaKey, Tracked.DELETE);
        return super.delete(key, version);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        checkRateLimit(getQuotaKey, Tracked.GET);
        return super.get(key, transforms);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        checkRateLimit(getAllQuotaKey, Tracked.GET_ALL);
        return super.getAll(keys, transforms);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        checkRateLimit(putQuotaKey, Tracked.PUT);
        super.put(key, value, transforms);
    }
}
