package voldemort.collections;

import java.util.List;
import java.util.Map;

import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.serialization.Serializer;
import voldemort.versioning.Versioned;

/**
 * @author jko
 *
 * Update the VLinkedPagedList index with a new node.
 */
public class UpdatePageIndex<I, LK extends Comparable<LK>> 
extends UpdateAction<I, List<Map<String, Object>>>
{
  private final I _identifier;
  private final LK _newIndex;
  private final Serializer<LK> _serializer;
  
  public UpdatePageIndex(I identifier, LK newKey, Serializer<LK> serializer)
  {
    _identifier = identifier;
    _newIndex = newKey;
    _serializer = serializer;
  }

  @Override
  public void update(StoreClient<I, List<Map<String, Object>>> storeClient)
  {
    Versioned<List<Map<String, Object>>> versionedIndex = storeClient.get(_identifier);
    
    List<Map<String, Object>> pageIndex = versionedIndex.getValue();
    VPageIndexEntry<LK> oldIndexEntry = VPageIndexEntry.valueOf(pageIndex.remove(0), _serializer);
    int updatedPageId = 0;
    if (pageIndex.size() >= 1)
    {
      updatedPageId = ((Integer) pageIndex.get(0).get("pageId")) + 1;
    } else 
    {
      updatedPageId = 1;
    }
    oldIndexEntry = new VPageIndexEntry<LK>(updatedPageId, 
                                        oldIndexEntry.getLastIndex(),
                                        _serializer);
    VPageIndexEntry<LK> newIndexEntry = new VPageIndexEntry<LK>(0, _newIndex, _serializer);
    pageIndex.add(0, oldIndexEntry.mapValue());
    pageIndex.add(0, newIndexEntry.mapValue());
    storeClient.put(_identifier, 
                    new Versioned<List<Map<String, Object>>>(pageIndex, versionedIndex.getVersion()));
  }

}
