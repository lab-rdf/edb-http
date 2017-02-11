package edu.columbia.rdf.edb.http;

import java.util.HashMap;
import java.util.Map;

public class ConcurrentKeyValueStore<T, V> implements ConcurrentStore {
	private Map<T, V> map = new HashMap<T, V>();
	
	public synchronized void put(T id, V value) {
		if (value == null) {
			return;
		}
		
		map.put(id, value);
	}
	
	public synchronized boolean contains(T id) {
		return map.containsKey(id);
	}
	
	public synchronized V get(T id) {
		return map.get(id);
	}

	@Override
	public synchronized void clear() {
		map.clear();
	}
}
