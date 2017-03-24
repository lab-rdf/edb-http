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

import java.nio.file.Path;

import org.abh.common.bioinformatics.annotation.Type;

// TODO: Auto-generated Javadoc
/**
 * The Class FileRecord.
 */
public class FileRecord extends Type {

	/** The m path. */
	private Path mPath;

	/**
	 * Instantiates a new file record.
	 *
	 * @param id the id
	 * @param name the name
	 * @param path the path
	 */
	public FileRecord(int id, String name, Path path) {
		super(id, name);
		
		mPath = path;
	}
	
	/**
	 * Gets the path.
	 *
	 * @return the path
	 */
	public Path getPath() {
		return mPath;
	}

}
