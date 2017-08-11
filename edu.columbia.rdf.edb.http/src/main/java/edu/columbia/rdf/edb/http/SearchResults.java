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

import java.util.Collection;
import java.util.Collections;

// TODO: Auto-generated Javadoc
/**
 * The Class SearchResults.
 */
public class SearchResults {
	
	private static final Collection<Integer> EMPTY_SET = Collections.emptySet();

	/** The m values. */
	private Collection<Integer> mValues;
	
	/** The m include. */
	private boolean mInclude;

	
	public SearchResults() {
		this(EMPTY_SET);
	}
	
	/**
	 * Instantiates a new search results.
	 *
	 * @param values the values
	 */
	public SearchResults(Collection<Integer> values) {
		this(values, true);
	}

	/**
	 * Instantiates a new search results.
	 *
	 * @param values the values
	 * @param include the include
	 */
	public SearchResults(Collection<Integer> values, boolean include) {
		mValues = values;
		mInclude = include;
	}
	
	/**
	 * Gets the values.
	 *
	 * @return the values
	 */
	public Collection<Integer> getValues() {
		return mValues;
	}
	
	/**
	 * Gets the include.
	 *
	 * @return the include
	 */
	public boolean getInclude() {
		return mInclude;
	}
}
