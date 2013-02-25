package org.apache.hadoop.hive.ql.parse.sql.generator;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Special HashMap for Generator Factory
 * GeneratorHashMap.
 *
 */
public class GeneratorHashMap implements Map<Integer, HiveASTGenerator> {

  private int size = 1000;
  private HiveASTGenerator kv[];

  public GeneratorHashMap() {
    init();
  }

  public GeneratorHashMap(int size) {
    this.size = size;
    init();
  }

  private void init() {
    kv = new HiveASTGenerator[size];
  }

  @Override
  public void clear() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean containsKey(Object key) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Set<java.util.Map.Entry<Integer, HiveASTGenerator>> entrySet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HiveASTGenerator get(Object key) {
    return kv[(Integer) key];
  }

  @Override
  public boolean isEmpty() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Set<Integer> keySet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HiveASTGenerator put(Integer key, HiveASTGenerator value) {
    if (this.get(key) != null) {
      throw new RuntimeException("Depulcate Generator-" + key);
    }

    kv[key] = value;
    return value;
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends HiveASTGenerator> m) {
    // TODO Auto-generated method stub

  }

  @Override
  public HiveASTGenerator remove(Object key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Collection<HiveASTGenerator> values() {
    // TODO Auto-generated method stub
    return null;
  }


}
