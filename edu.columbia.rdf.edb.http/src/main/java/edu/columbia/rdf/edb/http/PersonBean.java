package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "id", "first_name", "last_name", "address", "email" })
public class PersonBean extends Type {

  private String mLastName;
  private String mAddress;
  private String mEmail;

  public PersonBean() {
    super(-1, "");
  }

  public PersonBean(int id, String firstName, String lastName, String address,
      String email) {
    super(id, firstName);

    mLastName = lastName;
    mAddress = address;
    mEmail = email;
  }

  /*
  public void setId(int id) {
    mId = id;
  }

  public void setFirstName(String name) {
    mName = name;
  }

  public void setLastName(String name) {
    mLastName = name;
  }

  public void setAddress(String name) {
    mAddress = name;
  }

  public void setEmail(String name) {
    mEmail = name;
  }
  */

  @Override
  @JsonIgnore
  public String getName() {
    return super.getName();
  }

  @JsonGetter("first_name")
  public String getFirstName() {
    return getName();
  }

  @JsonGetter("last_name")
  public String getLastName() {
    return mLastName;
  }

  public String getAddress() {
    return mAddress;
  }

  public String getEmail() {
    return mEmail;
  }
}
