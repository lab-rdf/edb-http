package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

//@JsonPropertyOrder(alphabetic=true)
@JsonPropertyOrder({ "id", "e", "t", "n", "o", "d", "g" })
public class SampleBean extends Type {

  private String mDate;
  private int mExperimentId;
  private int mTypeId;
  private int mOrganism;
  //private Collection<Integer> mGroups;

  public SampleBean(int id, int experimentId, String name, int typeId,
      int organismId, String date) {
    super(id, name);

    mExperimentId = experimentId;
    mTypeId = typeId;
    mOrganism = organismId;
    mDate = date;
    //mGroups = groups; , Collection<Integer> groups

    /// samples.id,
    // samples.experiment_id,
    // samples.expression_type_id,
    // samples.name,
    // samples.organism_id,
    // TO_CHAR(samples.created, 'YYYY-MM-DD')
  }

  @JsonGetter("d")
  public String getDate() {
    return mDate;
  }

  @JsonGetter("e")
  public int getExperimentId() {
    return mExperimentId;
  }

  @JsonGetter("t")
  public int getType() {
    return mTypeId;
  }

  @JsonGetter("o")
  public int getOrganismId() {
    return mOrganism;
  }

  @Override
  @JsonGetter("n")
  public String getName() {
    return mName;
  }

  //@JsonGetter("g")
  //public Collection<Integer> getGroups() {
  //   return mGroups;
  //}
}
