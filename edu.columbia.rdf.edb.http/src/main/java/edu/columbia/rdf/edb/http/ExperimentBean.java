package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

//@JsonPropertyOrder(alphabetic=true)
@JsonPropertyOrder({ "id", "p", "n", "ds", "d" })
public class ExperimentBean extends Type {

  private String mPublicId;
  private String mDescription;
  private String mDate;

  public ExperimentBean(int id, String publicId, String name,
      String description, String date) {
    super(id, name);

    mPublicId = publicId;
    mDescription = description;
    mDate = date;
  }

  @JsonGetter("d")
  public String getDate() {
    return mDate;
  }

  @JsonGetter("p")
  public String getPublicId() {
    return mPublicId;
  }

  @JsonGetter("ds")
  public String getDescription() {
    return mDescription;
  }

  @Override
  @JsonGetter("n")
  public String getName() {
    return mName;
  }
}
