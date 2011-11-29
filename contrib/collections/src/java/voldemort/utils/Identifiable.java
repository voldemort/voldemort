package voldemort.utils;

import java.io.Serializable;

/**
 * Interface providing the identifier of an entity
 */
public interface Identifiable<K extends Serializable>
{
  public K getId();
}

