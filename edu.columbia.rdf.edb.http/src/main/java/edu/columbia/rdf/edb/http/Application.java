package edu.columbia.rdf.edb.http;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class Application {
	public static final String HEADING_EXPERIMENT_ID = "exp";
	public static final String HEADING_NAME = "name";
	public static final String HEADING_SAMPLE_ID = "sid";
	public static final String HEADING_ATTRIBUTE_ID = "attribute_id";
	public static final String HEADING_ARRAY_DESIGN_ID = "array_design_id";
	public static final String HEADING_PROVIDER_ID = "provider_id";
	public static final String HEADING_PERSON_ID = "pid";
	public static final String HEADING_ROLE_ID = "rid";
	public static final String HEADING_SECTION_TYPE_ID = "section_type_id";
	public static final String HEADING_KEYWORD_ID = "keyword_id";
	public static final String HEADING_FILE_TYPE_ID = "file_type_id";
	public static final String HEADING_ARRAY_DESIGN_NAME = "array_design_name";
	public static final String HEADING_PROVIDER_NAME = "provider_name";
	public static final String HEADING_ID = "id";
	public static final String HEADING_ATTRIBUTE_NAME = "attribute_name";
	public static final String HEADING_ATTRIBUTE_VALUE = "attribute_value";
	public static final String HEADING_TYPE = "type";
	public static final String HEADING_VALUE = "value";
	public static final String HEADING_ASSAY_NAME = "assay_name";
	public static final String HEADING_PATH = "path";
	public static final String HEADING_TAG_ID = "tag";
	public static final String HEADING_EXPRESSION_TYPE_ID = "expression_type_id";
	public static final String HEADING_EXPRESSION_TYPE = "expression_type";
	public static final String HEADING_GEO_ACCESSION = "accession";
	public static final String HEADING_GEO_SERIES_ID = "geo_series_id";
	public static final String HEADING_GEO_PLATFORM_ID = "geo_platform_id";
	
	//public static final String CONNECTION_POOL_ATTRIBUTE = "connection_pool";
	public static final String CONNECTION_ATTRIBUTE = "connection";
	public static final String DRIVER_ATTRIBUTE = "driver";
	public static final String USER_ATTRIBUTE = "user";
	public static final String USER_PASSWORD = "password";
	public static final String HEADING_PUBLIC_ID = "public_id";
	public static final String EXPERIMENT_VIEW_ATTRIBUTE = "experiment_view";
	//public static final String SAMPLE_STORE_ATTRIBUTE = "sample_store";
	public static final String AUTHENTICATION_STORE_ATTRIBUTE = "authentication_store";
	public static final String SECTION_TYPES_STORE_ATTRIBUTE = "section_types_store";
	public static final String ATTRIBUTE_ID = "attribute_id";
	public static final String ATTRIBUTES_STORE_ATTRIBUTE = "attributes_store";
	public static final String KEYWORDS_STORE_ATTRIBUTE = "attributes_keywords";
	public static final String SAMPLE_EXPERIMENT_STORE_ATTRIBUTE = "attributes_sample_experiment";
	public static final String PUBLIC_ID_STORE_ATTRIBUTE = "attributes_public_id";
	public static final String VERSION_ATTRIBUTE = "attribute_version";
	public static final String SAMPLE_VIEW_ATTRIBUTE = "sample_view_attribute";
	public static final String FILE_VIEW_ATTRIBUTE = "file_view_attribute";
	public static final String DATA_VIEW_ATTRIBUTE = "data_view";
	public static final String HEADING_TYPE_ID = "type_id";
	public static final String HEADING_DESCRIPTION = "description";
	public static final String HEADING_RELEASED = "release";
	public static final String HEADING_CREATED = "created";
	public static final String HEADING_TAGS = "tags";
	public static final String HEADING_FILES = "files";
	public static final String HEADING_SPECIES = "species";
	public static final String HEADING_UNLOCKED = "unlocked";
	public static final String HEADING_LOCKED = "locked";
	public static final String HEADING_STATE = "state";
	
	private static Pattern NUMERICAL_ID_PATTERN = 
			Pattern.compile("^\\d{0,20}$");
	
	private static Pattern STRING_ID_PATTERN = 
			Pattern.compile("^[A-Za-z0-9\\_\\-\\.\\%]+$");
	
	private static Pattern GEO_ACCESSION_PATTERN = 
			Pattern.compile("^G[A-Z]{2}\\d+$");
	

	private Application() {
		// do nothing
	}
	
	
	
	/**
	 * Validates text to ensure it conforms to a textual id and contains
	 * no illegal characters.
	 * 
	 * @param id
	 * @return
	 */
	public static boolean isValidId(String idParameter) {
		if (idParameter == null) {
			return false;
		}
		
		return STRING_ID_PATTERN.matcher(idParameter).matches();
	}
	
	/**
	 * Checks that a parameter consists only of digits. If the id is
	 * invalid, -1 will be returned.
	 * 
	 * @param idParameter
	 * @return
	 * @throws InvalidIdException
	 */
	public static int validateId(String idParameter) {
		if (!isValidNumericalId(idParameter)) {
			return -1;
		}
		
		return Integer.parseInt(idParameter);
	}
	
	/**
	 * Determines if an id is a valid geo id or not.
	 * 
	 * @param idParameter
	 * @return
	 */
	public static String validateGeoAccession(String idParameter) {
		if (!GEO_ACCESSION_PATTERN.matcher(idParameter).matches()) {
			return null;
		}
		
		return idParameter;
	}
	
	/**
	 * Validates an id to ensure it is numerical and not arbitrary rubbish.
	 * 
	 * @param id
	 * @return
	 */
	public static boolean isValidNumericalId(String idParameter) {
		return NUMERICAL_ID_PATTERN.matcher(idParameter).matches();
	}
	
	public static Object lookup(String name) throws NamingException {
		Context initCtx = new InitialContext();
		
		Context envCtx = (Context)initCtx.lookup("java:comp/env");
		
		return envCtx.lookup(name);
	}
	
	
	
	public static DataSource getDatabasePool() throws NamingException {
		return ((DataSource)Application.lookup("jdbc/experimentdb"));
	}
	
	public static Connection getConnection() throws SQLException, NamingException {
		return getDatabasePool().getConnection();
	}
}
