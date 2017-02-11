package edu.columbia.rdf.edb.http;

import org.abh.common.json.JsonObject;

public class JsonAuthFailed extends JsonObject {
	public JsonAuthFailed(String message) {
		add("auth-failed", message);
	}

}
