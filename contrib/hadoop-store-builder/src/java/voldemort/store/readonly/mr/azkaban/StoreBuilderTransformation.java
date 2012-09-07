package voldemort.store.readonly.mr.azkaban;

/**
 * An interface to use for processing rows in the voldemort store builder
 * 
 * @author jkreps
 *
 */
public interface StoreBuilderTransformation
{
  
  public Object transform(Object obj);

}
