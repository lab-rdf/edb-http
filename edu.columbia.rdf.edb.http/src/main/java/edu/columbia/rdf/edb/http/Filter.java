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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.abh.common.collections.CollectionUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class Search.
 */
public class Filter {
	private Filter() {
		// Do nothing
	}

	/**
	 * Filter a list of samples to only contain those of a specific type.
	 * 
	 * @param samples
	 * @param types
	 * @return
	 */
	public static List<SampleBean> filterByTypes(List<SampleBean> samples, 
			Collection<Integer> types) {
		if (CollectionUtils.isNullOrEmpty(types)) {
			return samples;
		}
		
		List<SampleBean> ret = new ArrayList<SampleBean>(samples.size());
		
		for (SampleBean sample : samples) {
			int type = sample.getType();
			
			if (types.contains(type)) {
				ret.add(sample);
			}
		}
		
		return ret;
	}
	
	public static List<SampleBean> filterByOrganisms(List<SampleBean> samples, 
			Collection<Integer> organisms) {
		if (CollectionUtils.isNullOrEmpty(organisms)) {
			return samples;
		}
		
		List<SampleBean> ret = new ArrayList<SampleBean>(samples.size());
		
		for (SampleBean sample : samples) {
			int organism = sample.getOrganismId();
			
			if (organisms.contains(organism)) {
				ret.add(sample);
			}
		}
		
		return ret;
	}
}
