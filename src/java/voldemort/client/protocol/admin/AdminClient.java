package voldemort.client.protocol.admin;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.client.protocol.VoldemortFilter;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.versioning.Versioned;


public interface AdminClient {

    /**
     * streaming API to get all entries belonging to any of the partition in the
     * input List.
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList: List of partitions to be fetched from remote
     *        server.
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null
     * @return
     * @throws VoldemortException
     */
    public abstract Iterator<Pair<ByteArray, Versioned<byte[]>>> fetchPartitionEntries(int nodeId,
                                                                                       String storeName,
                                                                                       List<Integer> partitionList,
                                                                                       VoldemortFilter filter);

    /**
     * streaming API to get a list of all the keys that belong to any of the
     * partitions in the input list
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter
     * @return
     */
    public abstract Iterator<ByteArray> fetchPartitionKeys(int nodeId,
                                                           String storeName,
                                                           List<Integer> partitionList,
                                                           VoldemortFilter filter);

    /**
     * update Entries at (remote) node with all entries in iterator for passed
     * storeName
     * 
     * @param nodeId
     * @param storeName
     * @param entryIterator
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null.
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract void updatePartitionEntries(int nodeId,
                                                String storeName,
                                                Iterator<Pair<ByteArray, Versioned<byte[]>>> entryIterator,
                                                VoldemortFilter filter);

    /**
     * Delete all Entries at (remote) node for partitions in partitionList
     * 
     * @param nodeId
     * @param storeName
     * @param partitionList
     * @param filter: A VoldemortFilter class to do server side filtering or
     *        null.
     * @throws VoldemortException
     * @throws IOException
     */
    public abstract int deletePartitionEntries(int nodeId,
                                               String storeName,
                                               List<Integer> partitionList,
                                               VoldemortFilter filter);

}
