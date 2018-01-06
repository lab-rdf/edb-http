package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "n" })
public class ReadOnlyTypeBean extends Type {

  public ReadOnlyTypeBean() {
    this(-1, "");
  }

  public ReadOnlyTypeBean(int id, String name) {
    super(id, name);
  }

  @Override
  @JsonGetter("n")
  public String getName() {
    return super.getName();
  }
}
