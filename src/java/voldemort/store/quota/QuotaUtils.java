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

import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import voldemort.VoldemortApplicationException;
import voldemort.server.StoreRepository;
import voldemort.store.configuration.FileBackedCachingStorageEngine;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.versioning.VectorClock;
import voldemort.versioning.VectorClockUtils;
import voldemort.versioning.Versioned;


public class QuotaUtils {

  private static final String QUOTA_STORE = SystemStoreConstants.SystemStoreName.voldsys$_store_quotas.toString();

    public static String makeQuotaKey(String storeName, QuotaType quotaType) {
        return String.format("%s.%s", storeName, quotaType.toString());
    }

    public static ByteArray getByteArrayKey(String storeName, QuotaType quotaType) {
        String quotaKey = makeQuotaKey(storeName, quotaType);

        try {
            return new ByteArray(quotaKey.getBytes("UTF8"));
        } catch (UnsupportedEncodingException ex) {
            throw new VoldemortApplicationException("Error converting key" + quotaKey, ex);
        }
    }

    public static Set<String> validQuotaTypes() {
        HashSet<String> quotaTypes = new HashSet<String>();
        for(QuotaType type: QuotaType.values()) {
            quotaTypes.add(type.toString());
        }
        return quotaTypes;
    }

  private static FileBackedCachingStorageEngine getQuotaStore(StoreRepository repository) {
    FileBackedCachingStorageEngine quotaStore =
        (FileBackedCachingStorageEngine) repository.getStorageEngine(QUOTA_STORE);

    if (quotaStore == null) {
      throw new VoldemortApplicationException("Could not find the quota store for Store " + QUOTA_STORE);
    }

    return quotaStore;
  }

  private static ByteArray convertToByteArray(String value) {
    try {
      return new ByteArray(value.getBytes("UTF8"));
    } catch (UnsupportedEncodingException ex) {
      throw new VoldemortApplicationException("Error converting key to byte array " + value, ex);
    }
  }

  public static Long getQuota(String storeName, QuotaType type, StoreRepository repository) {
    FileBackedCachingStorageEngine quotaStore = getQuotaStore(repository);
    String quotaKey = makeQuotaKey(storeName, QuotaType.STORAGE_SPACE);
    String diskQuotaSize = quotaStore.cacheGet(quotaKey);
    Long diskQuotaSizeInKB = (diskQuotaSize == null) ? null : Long.parseLong(diskQuotaSize);
    return diskQuotaSizeInKB;
  }

  public static void setQuota(String storeName, QuotaType type, StoreRepository repository, Set<Integer> nodeIds,
      long lquota) {
    FileBackedCachingStorageEngine quotaStore = getQuotaStore(repository);
    String quotaKey = makeQuotaKey(storeName, QuotaType.STORAGE_SPACE);
    ByteArray keyArray = convertToByteArray(quotaKey);

    List<Versioned<byte[]>> existingValue = quotaStore.get(keyArray, null);
    String quotaValue = Long.toString(lquota);

    ByteArray valueArray = convertToByteArray(quotaValue);
    VectorClock newClock = VectorClockUtils.makeClockWithCurrentTime(nodeIds);
    Versioned<byte[]> newValue = new Versioned<byte[]>(valueArray.get(), newClock);
    quotaStore.put(keyArray, newValue, null);

  }
}
