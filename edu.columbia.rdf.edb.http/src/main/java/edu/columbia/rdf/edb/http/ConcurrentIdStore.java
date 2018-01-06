/**
 * Copyright 2017 Antony Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.columbia.rdf.edb.http;

import java.util.HashMap;
import java.util.Map;

// TODO: Auto-generated Javadoc
/**
 * The Class ConcurrentIdStore.
 */
public class ConcurrentIdStore implements ConcurrentStore {

  /** The map. */
  private Map<Integer, Map<Integer, Boolean>> map = new HashMap<Integer, Map<Integer, Boolean>>();

  /**
   * Put.
   *
   * @param keyId
   *          the key id
   * @param id
   *          the id
   * @param value
   *          the value
   */
  public synchronized void put(int keyId, int id, boolean value) {
    if (!map.containsKey(keyId)) {
      map.put(keyId, new HashMap<Integer, Boolean>());
    }

    map.get(keyId).put(id, value);
  }

  /**
   * Contains.
   *
   * @param keyId
   *          the key id
   * @param id
   *          the id
   * @return true, if successful
   */
  public synchronized boolean contains(int keyId, int id) {
    if (!map.containsKey(keyId)) {
      return false;
    }

    return map.get(keyId).containsKey(id);
  }

  /**
   * Gets the.
   *
   * @param keyId
   *          the key id
   * @param id
   *          the id
   * @return true, if successful
   */
  public synchronized boolean get(int keyId, int id) {
    if (!map.containsKey(keyId)) {
      return false;
    }

    return map.get(keyId).get(id);
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.columbia.rdf.edb.http.ConcurrentStore#clear()
   */
  @Override
  public synchronized void clear() {
    map.clear();
  }
}
