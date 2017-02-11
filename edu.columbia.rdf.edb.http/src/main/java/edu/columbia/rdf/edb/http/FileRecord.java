package edu.columbia.rdf.edb.http;

import java.nio.file.Path;

import org.abh.common.bioinformatics.annotation.Type;

public class FileRecord extends Type {

	private Path mPath;

	public FileRecord(int id, String name, Path path) {
		super(id, name);
		
		mPath = path;
	}
	
	public Path getPath() {
		return mPath;
	}

}
