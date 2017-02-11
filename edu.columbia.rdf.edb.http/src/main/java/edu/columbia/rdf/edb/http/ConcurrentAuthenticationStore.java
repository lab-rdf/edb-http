package edu.columbia.rdf.edb.http;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores mappings of keys, ip addresses and person ids to lower
 * requests to the database.
 * 
 * @author Antony Holmes
 *
 */
public class ConcurrentAuthenticationStore implements ConcurrentStore {
	private Map<String, Map<String, Integer>> map = new HashMap<String, Map<String, Integer>>();
	
	public synchronized void put(String key, String ipAddress, int personId) {
		if (!map.containsKey(key)) {
			map.put(key, new HashMap<String, Integer>());
		}
		
		map.get(key).put(ipAddress, personId);
	}
	
	/**
	 * Returns the person id associated with the key mapping to a given 
	 * ip address. If either the key and ip address do not partner, -1
	 * will be returned indicating this is not a valid authentication.
	 * 
	 * @param key
	 * @param ipAddress
	 * @return
	 */
	public synchronized boolean contains(String key, String ipAddress) {
		if (!map.containsKey(key)) {
			return false;
		}
		
		return map.get(key).containsKey(ipAddress);
	}
	
	/**
	 * Returns the person associated with a key for a given ip address.
	 * There is no check on whether the key and ip address have a match
	 * for speed. Use contains() to check there is a valid mapping before
	 * calling this method. This was done so that only one check
	 * is required to test if a mapping exists, otherwise contains()
	 * would be run twice, once to check if a mapping exists and again
	 * when requesting the person id.
	 * 
	 * @param key
	 * @param ipAddress
	 * @return
	 */
	public synchronized int get(String key, String ipAddress) {
		//if (!contains(key, ipAddress)) {
		//	return -1;
		//}
		
		return map.get(key).get(ipAddress);
	}

	@Override
	public void clear() {
		map.clear();
	}
}
