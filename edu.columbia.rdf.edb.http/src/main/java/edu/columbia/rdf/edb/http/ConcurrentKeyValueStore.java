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
 * The Class ConcurrentKeyValueStore.
 *
 * @param <T> the generic type
 * @param <V> the value type
 */
public class ConcurrentKeyValueStore<T, V> implements ConcurrentStore {
	
	/** The map. */
	private Map<T, V> map = new HashMap<T, V>();
	
	/**
	 * Put.
	 *
	 * @param id the id
	 * @param value the value
	 */
	public synchronized void put(T id, V value) {
		if (value == null) {
			return;
		}
		
		map.put(id, value);
	}
	
	/**
	 * Contains.
	 *
	 * @param id the id
	 * @return true, if successful
	 */
	public synchronized boolean contains(T id) {
		return map.containsKey(id);
	}
	
	/**
	 * Gets the.
	 *
	 * @param id the id
	 * @return the v
	 */
	public synchronized V get(T id) {
		return map.get(id);
	}

	/* (non-Javadoc)
	 * @see edu.columbia.rdf.edb.http.ConcurrentStore#clear()
	 */
	@Override
	public synchronized void clear() {
		map.clear();
	}
}
