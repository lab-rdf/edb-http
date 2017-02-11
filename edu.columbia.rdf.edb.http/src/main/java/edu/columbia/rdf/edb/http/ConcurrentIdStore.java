package edu.columbia.rdf.edb.http;

import java.util.HashMap;
import java.util.Map;

public class ConcurrentIdStore implements ConcurrentStore {
	private Map<Integer, Map<Integer, Boolean>> map = new HashMap<Integer, Map<Integer, Boolean>>();
	
	public synchronized void put(int keyId, int id, boolean value) {
		if (!map.containsKey(keyId)) {
			map.put(keyId, new HashMap<Integer, Boolean>());
		}
		
		map.get(keyId).put(id, value);
	}
	
	public synchronized boolean contains(int keyId, int id) {
		if (!map.containsKey(keyId)) {
			return false;
		}
		
		return map.get(keyId).containsKey(id);
	}
	
	public synchronized boolean get(int keyId, int id) {
		if (!map.containsKey(keyId)) {
			return false;
		}
		
		return map.get(keyId).get(id);
	}
	
	@Override
	public synchronized void clear() {
		map.clear();
	}
}
