package voldemort.collections;

import java.util.Map;

import voldemort.client.StoreClient;
import voldemort.client.UpdateAction;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * @author jko
 *
 * Put the given value to the appropriate id in the stack, using the version of the current
 * list node identified by that id.
 * 
 * @param id
 * @param element element to set
 * @return element that was replaced by the new element
 */
public class UpdateElementById<K, E> extends UpdateAction<Map<String, Object>, Map<String, Object>>
{
  private final VListKey<K> _key;
  private final E _element;
  private final Version _version;
  private E _result = null;
 
  public UpdateElementById(VListKey<K> key, E element)
  {
    _key = key;
    _element = element;
    _version = null;
  }
  
  public UpdateElementById(VListKey<K> key, Versioned<E> element)
  {
    _key = key;
    _element = element.getValue();
    _version = element.getVersion();
  }

  @Override
  public void update(StoreClient<Map<String, Object>, Map<String, Object>> storeClient)
  {
    Versioned<Map<String, Object>> nodeMap = storeClient.get(_key.mapValue());
    if (nodeMap == null)
      throw new IndexOutOfBoundsException("invalid id " + _key.getId());
    Version version = (_version != null) ? _version : nodeMap.getVersion();
    VListNode<E> listNode = VListNode.valueOf(nodeMap.getValue());
    if (! listNode.isStable())
    {
      throw new ObsoleteVersionException("node " + _key.getId() + " not stable.");
    }
    _result = listNode.getValue();
    VListNode<E> newNode = new VListNode<E>(_element, listNode.getId(), listNode.getPreviousId(),
        listNode.getNextId(), true);
    storeClient.put(_key.mapValue(),
                    new Versioned<Map<String, Object>>(newNode.mapValue(), version));
                        
  }
  
  public E getResult() 
  {
    return _result;
  }

}
