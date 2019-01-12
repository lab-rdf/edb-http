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

/**
 * Stores mappings of keys, ip addresses and person ids to lower requests to the
 * database.
 * 
 * @author Antony Holmes
 *
 */
public class ConcurrentAuthenticationStore implements ConcurrentStore {

  /** The map. */
  private Map<String, Map<String, Integer>> map = new HashMap<String, Map<String, Integer>>();

  /**
   * Put.
   *
   * @param key the key
   * @param ipAddress the ip address
   * @param personId the person id
   */
  public synchronized void put(String key, String ipAddress, int personId) {
    if (!map.containsKey(key)) {
      map.put(key, new HashMap<String, Integer>());
    }

    map.get(key).put(ipAddress, personId);
  }

  /**
   * Returns the person id associated with the key mapping to a given ip
   * address. If either the key and ip address do not partner, -1 will be
   * returned indicating this is not a valid authentication.
   *
   * @param key the key
   * @param ipAddress the ip address
   * @return true, if successful
   */
  public synchronized boolean contains(String key, String ipAddress) {
    if (!map.containsKey(key)) {
      return false;
    }

    return map.get(key).containsKey(ipAddress);
  }

  /**
   * Returns the person associated with a key for a given ip address. There is
   * no check on whether the key and ip address have a match for speed. Use
   * contains() to check there is a valid mapping before calling this method.
   * This was done so that only one check is required to test if a mapping
   * exists, otherwise contains() would be run twice, once to check if a mapping
   * exists and again when requesting the person id.
   *
   * @param key the key
   * @param ipAddress the ip address
   * @return the int
   */
  public synchronized int get(String key, String ipAddress) {
    // if (!contains(key, ipAddress)) {
    // return -1;
    // }

    return map.get(key).get(ipAddress);
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.columbia.rdf.edb.http.ConcurrentStore#clear()
   */
  @Override
  public void clear() {
    map.clear();
  }
}
