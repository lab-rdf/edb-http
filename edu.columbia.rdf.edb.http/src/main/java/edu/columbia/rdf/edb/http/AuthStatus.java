package edu.columbia.rdf.edb.http;

public enum AuthStatus {
  INVALID_USER, INVALID_TOTP, INVALID_KEY_FORMAT, SUCCESS, VALID_USER, VALID_KEY_FORMAT;
}
