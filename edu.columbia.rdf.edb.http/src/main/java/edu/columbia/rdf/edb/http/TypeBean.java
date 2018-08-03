package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "n" })
public class TypeBean extends Type {

  public TypeBean(int id, String name) {
    super(id, name);
  }
}
