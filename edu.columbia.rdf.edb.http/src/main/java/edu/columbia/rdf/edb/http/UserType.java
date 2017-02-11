package edu.columbia.rdf.edb.http;

public enum UserType {
	NORMAL,
	ADMINISTRATOR,
	SUPERUSER;
	
	public static UserType getFromId(int userTypeId) {
		switch(userTypeId) {
		case 4:
			return SUPERUSER;
		case 3:
			return ADMINISTRATOR;
		default:
			return NORMAL;
		}
	}
}
