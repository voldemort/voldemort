package voldemort.collections;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;

/**
 * @author jko
 *
 * Adds a node E to the front of the list identified by K. Inserts the node to the front
 * of the list, with id 0, and then modifies the pointers for the first node and next
 * node accordingly.
 * 
 * @param <E> the type of the value
 * @param <K> the type of the key identifying the list
 */
public class AddNodeAction<K, E> extends UpdateAction<Map<String, Object>, Map<String, Object>>
{
  private final K _key;
  private final E _value;
  
  @SuppressWarnings("unchecked")
  private StoreClient<Map<String, Object>, Map<String, Object>> _storeClient = null;
  private Map<Integer, Map<String, Object>> _rollback = new HashMap<Integer, Map<String, Object>>();
  
  public AddNodeAction(K key, E value)
  {
    _key = key;
    _value = value;
  }
  
  @Override
  public void rollback() 
  {
    if (_storeClient == null)
      return;
    
    for (Entry<Integer, Map<String, Object>> entry : _rollback.entrySet()) 
    {
      VListKey<K> key = new VListKey<K>(_key, entry.getKey());
      _storeClient.put(key.mapValue(), entry.getValue());
    }
  }

  /**
   * @throws ObsoleteVersionException if a concurrent modification (remove or another add)
   * has not completed modifying the structure.
   * @throws ArrayIndexOutOfBoundsException if no more ids left to identify object
   */
  @Override
  public void update(StoreClient<Map<String, Object>, Map<String, Object>> storeClient)
  {
    _storeClient = storeClient;
    VListKey<K> newKey = new VListKey<K>(_key, 0);
    Versioned<Map<String, Object>> firstNodeMap =  storeClient.get(newKey.mapValue());
    // adding first node of the list
    if (firstNodeMap == null)
    {
      Versioned<Map<String, Object>> newNode = 
        new Versioned<Map<String, Object>>((new VListNode<E>(_value, 0, VStack.NULL_ID, 
                                                             VStack.NULL_ID, true)).mapValue());
      // throws ObsoleteVersionException if another process has created a new node already
      storeClient.put(newKey.mapValue(), newNode);
    } 
    else // add to front of list
    {
      Versioned<VListNode<E>> firstNode = 
        new Versioned<VListNode<E>>(VListNode.<E>valueOf(firstNodeMap.getValue()), 
            firstNodeMap.getVersion());
      
      if (! firstNode.getValue().isStable())
      {
        throw new ObsoleteVersionException("cannot add when list node is not stable");
      }

      // set stable flag to false
      Map<String, Object> tmpMap = new HashMap<String, Object>(firstNodeMap.getValue());
      tmpMap.put(VListNode.STABLE, false);
      storeClient.put(newKey.mapValue(), 
                      new Versioned<Map<String, Object>>(tmpMap, firstNodeMap.getVersion()));
      _rollback.put(0, firstNodeMap.getValue());
      
      int newId;
      int nextId = firstNode.getValue().getNextId();
      newId = (nextId == VStack.NULL_ID) ? 1 : nextId + 1;
      if (newId == Integer.MAX_VALUE)
        throw new ArrayIndexOutOfBoundsException(newId + " out of bounds");
      
      Versioned<VListNode<E>> nextNode = null;
      VListKey<K> nextKey = new VListKey<K>(_key, nextId);
      if (nextId != VStack.NULL_ID)
      {
        Versioned<Map<String, Object>> nextNodeMap = storeClient.get(nextKey.mapValue());
        if (nextNodeMap == null)
          throw new ObsoleteVersionException("possible concurrent modification");
        nextNode = new Versioned<VListNode<E>>(VListNode.<E>valueOf(nextNodeMap.getValue()),
            nextNodeMap.getVersion());
        if (! nextNode.getValue().isStable()) 
        {
          throw new ObsoleteVersionException("cannot add when list node is not stable");
        }
        
        // set stable flag to false
        tmpMap = new HashMap<String, Object>(nextNode.getValue().mapValue());
        tmpMap.put(VListNode.STABLE, false);
        storeClient.put(nextKey.mapValue(), 
                        new Versioned<Map<String, Object>>(tmpMap, nextNode.getVersion()));
        _rollback.put(nextId, nextNode.getValue().mapValue());
      }

      // insert new node
      Map<String, Object> newNode = 
        (new VListNode<E>(_value, 0, VStack.NULL_ID, newId, true)).mapValue();
      // don't need to specify versioned because node is already "locked"
      storeClient.put(newKey.mapValue(), newNode);
      
      // move first node to next index
      VListKey<K> firstKey = new VListKey<K>(_key, newId);
      firstNode = new Versioned<VListNode<E>>(new VListNode<E>(firstNode.getValue().getValue(),
          newId, 0, firstNode.getValue().getNextId(), true));
      // don't need to specify versioned because node is already "locked"
      storeClient.put(firstKey.mapValue(), 
                      firstNode.getValue().mapValue());
      
      // redefine previous pointer on next node
      if (nextNode != null)
      {
        if (! storeClient.applyUpdate(new UpdateNextNode<K, E>(nextNode, nextKey, newId)))
          throw new ObsoleteVersionException("unable to update node");
      }
    }
  }
  
  /**
   * Updates a the previous pointer of a node specified by key to a newId.
   *
   * @param <K>
   * @param <E>
   */
  private static class UpdateNextNode<K, E> 
  extends UpdateAction<Map<String, Object>, Map<String, Object>>
  {
    private final VListKey<K> _key;
    private final int _newId;

    private Versioned<VListNode<E>> _listNode;
    private int numCalls = 0;
    
    /**
     * @param listNode 
     * @param key
     * @param newId
     */
    public UpdateNextNode(Versioned<VListNode<E>> listNode, VListKey<K> key, int newId)
    {
      _listNode = listNode;
      _key = key;
      _newId = newId;
    }

    @Override
    public void update(StoreClient<Map<String, Object>, Map<String, Object>> storeClient)
    {
      if (numCalls > 0) 
      {
        // TODO jko maybe delete this if unnecessary
        Versioned<Map<String, Object>> nextNodeMap = storeClient.get(_key.mapValue());
        if (nextNodeMap == null)
          throw new ObsoleteVersionException("possible concurrent modification");
        _listNode =  new Versioned<VListNode<E>>(VListNode.<E>valueOf(nextNodeMap.getValue()),
            nextNodeMap.getVersion());
      }
      
      VListNode<E> nodeValue = _listNode.getValue();
      _listNode.setObject(new VListNode<E>(nodeValue.getValue(), nodeValue.getId(),
          _newId, nodeValue.getNextId(), true));
      Map<String, Object> nextNodeMap = _listNode.getValue().mapValue();
      storeClient.put(_key.mapValue(), nextNodeMap);      
      
      numCalls++;
    }
    
  }
}
