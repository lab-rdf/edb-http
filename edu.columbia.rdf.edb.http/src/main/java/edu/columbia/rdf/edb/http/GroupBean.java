package edu.columbia.rdf.edb.http;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "n", "c"})
public class GroupBean extends TypeBean {

	private String mColor;

	public GroupBean(int id, String name, String color) {
		super(id, name);
		
		mColor = color;
	}
	
	@JsonGetter("c")
	public String getColor() {
		return mColor;
	}
}
