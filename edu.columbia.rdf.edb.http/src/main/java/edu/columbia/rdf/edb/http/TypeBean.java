package edu.columbia.rdf.edb.http;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "n" })
public class TypeBean extends ReadOnlyTypeBean {

  public TypeBean() {
    this(-1, "");
  }

  public TypeBean(int id, String name) {
    super(id, name);
  }

  public void setId(int id) {
    mId = id;
  }

  public void setName(String name) {
    mName = name;
  }
}
