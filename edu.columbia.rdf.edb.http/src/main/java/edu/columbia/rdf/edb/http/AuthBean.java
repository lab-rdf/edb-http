package edu.columbia.rdf.edb.http;

import org.jebtk.bioinformatics.annotation.Entity;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"key", "status", })
public class AuthBean extends Entity {
  private AuthStatus mStatus = AuthStatus.INVALID_USER;
  private UserType mUserType = UserType.NORMAL;
  private String mKey;

  public AuthBean(int id) {
    super(id);
  }

  public AuthBean(int id, AuthStatus status) {
    this(id);
    
    mStatus = status;
  }
  
  public AuthBean(AuthStatus status) {
    this(-1, status);
  }

  public AuthBean(String key, AuthStatus status) {
    this(-1, key, status);
  }
  
  public AuthBean(int id, String key, AuthStatus status) {
    this(id, status);
    
    mKey = key;
  }

  public AuthBean(int id, String key, UserType userType) {
    this(id, AuthStatus.SUCCESS);
    
    mKey = key;
    mUserType = userType;
  }
  
  @Override
  @JsonIgnore
  public int getId() {
    return super.getId();
  }

  @JsonGetter("status")
  public AuthStatus getStatus() {
    return mStatus;
  }

  @JsonIgnore
  public UserType getUserType() {
    return mUserType;
  }
  
  @JsonGetter("key")
  public String getKey() {
    return mKey;
  }

  public static AuthBean invalidKeyFormat(String key) {
    return new AuthBean(key, AuthStatus.INVALID_KEY_FORMAT);
  }
  
  public static AuthBean validKeyFormat(String key) {
    return new AuthBean(key, AuthStatus.VALID_KEY_FORMAT);
  }

  public static AuthBean invalidUser(String key) {
    return new AuthBean(key, AuthStatus.INVALID_USER);
  }
}
