package edu.columbia.rdf.edb.http;

public class Auth {
  private int mId;
  private AuthStatus mStatus = AuthStatus.INVALID_USER;
  private UserType mUserType;

  public Auth(int id) {
    mId = id;
  }

  public Auth(int id, AuthStatus status) {
    mId = id;
    mStatus = status;
  }

  public Auth(int id, UserType userType) {
    mId = id;
    mStatus = AuthStatus.SUCCESS;
    mUserType = userType;
  }

  public AuthStatus getStatus() {
    return mStatus;
  }

  public UserType getUserType() {
    return mUserType;
  }

  public int getUserId() {
    return mId;
  }
}
