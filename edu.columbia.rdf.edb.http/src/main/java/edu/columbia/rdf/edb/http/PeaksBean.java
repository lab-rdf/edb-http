package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Entity;
import org.jebtk.core.NameProperty;
import org.jebtk.core.json.JsonBuilder;
import org.jebtk.core.text.TextUtils;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRawValue;

@JsonPropertyOrder({ "id", "name", "genome", "params", "locations" })
public class PeaksBean extends Entity implements NameProperty {

  private String mName;
  private String mGenome;
  private String mParams;
  private String mPeaksJson;

  public PeaksBean(int id, String name, String genome) {
    this(id, name, genome, null);
  }

  public PeaksBean(int id, String name, String genome, String params) {
    this(id, name, genome, params, null);
  }

  public PeaksBean(int id, String name, String genome, String params,
      String peaks) {
    super(id);

    mName = name;
    mGenome = genome;
    mParams = TextUtils.isNullOrEmpty(params, JsonBuilder.JSON_EMPTY_ARRAY);
    mPeaksJson = TextUtils.isNullOrEmpty(peaks, JsonBuilder.JSON_EMPTY_ARRAY);
  }

  @Override
  //@JsonGetter("n")
  public String getName() {
    return mName;
  }

  //@JsonGetter("g")
  public String getGenome() {
    return mGenome;
  }

  //@JsonGetter("p")
  @JsonRawValue
  public String getParams() {
    return mParams;
  }

  //@JsonGetter("l")
  @JsonRawValue
  public String getLocations() {
    return mPeaksJson;
  }
}
