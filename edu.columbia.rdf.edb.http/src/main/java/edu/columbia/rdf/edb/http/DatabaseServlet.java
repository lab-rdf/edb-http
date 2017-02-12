package edu.columbia.rdf.edb.http;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.abh.common.bioinformatics.annotation.Species;
import org.abh.common.bioinformatics.annotation.Type;
import org.abh.common.collections.CollectionUtils;
import org.abh.common.database.DatabaseResultsTable;
import org.abh.common.database.JDBCConnection;
import org.abh.common.json.JsonArray;
import org.abh.common.json.JsonBuilder;
import org.abh.common.json.JsonObject;
import org.abh.common.path.Path;
import org.abh.common.text.TextUtils;

import edu.columbia.rdf.edb.Experiment;
import edu.columbia.rdf.edb.Person;
import edu.columbia.rdf.edb.Sample;
import edu.columbia.rdf.edb.SampleState;
import edu.columbia.rdf.edb.TypeMap;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;


/**
 * Provides a connection to the database for servlets and
 * deals with closing the connection.
 * 
 * @author Antony Holmes
 *
 */
public abstract class DatabaseServlet  {
	//private Pattern KEY_PATTERN = Pattern.compile("^[\\w\\-]+$");

	private static final String EXPERIMENT_PUBLIC_ID_SQL = 
			"SELECT experiments.public_id FROM experiments WHERE experiments.id = ?";

	private static final String TAG_ID_SQL = 
			"SELECT fields.id FROM fields WHERE fields.name = ?";

	private static final String EXPERIMENT_SAMPLES_SQL = 
			"SELECT samples.id FROM samples WHERE samples.experiment_id = ?";

	private static final String VERSION_SQL =
			"SELECT version.id, EXTRACT(EPOCH FROM version.version) FROM version ORDER BY version.version DESC LIMIT 1";

	private static final String GEO_SAMPLE_SQL = 
			"SELECT geo_samples.sample_id FROM geo_samples FROM geo_samples WHERE geo_samples.name = ?";

	

	private static final String SAMPLE_TAG_SQL = 
			"SELECT sample_fields.id, sample_fields.field_id, sample_fields.value FROM sample_fields WHERE sample_fields.sample_id = ?";

	/**
	 * Returns just the specific tags mentioned
	 */
	private static final String SAMPLE_SPECIFIC_TAGS_SQL = 
			SAMPLE_TAG_SQL + "AND sample_fields.field_id = ANY(?::int[])";

	private static final String JSON_SAMPLE_FIELDS_SQL = 
			"SELECT json_sample_fields.json FROM json_sample_fields WHERE json_sample_fields.sample_id = ?";

	private static final String JSON_SAMPLE_GEO_SQL = 
			"SELECT json_sample_geo.json FROM json_sample_geo WHERE json_sample_geo.sample_id = ?";

	private static final String JSON_SAMPLE_PERSONS_SQL = 
			"SELECT json_sample_persons.json FROM json_sample_persons WHERE json_sample_persons.sample_id = ?";

	
	private static final String SAMPLE_INT_TAG_SQL = 
			"SELECT sample_int_fields.id, sample_int_fields.field_id, sample_int_fields.value FROM sample_int_fields WHERE sample_int_fields.sample_id = ?";

	private static final String SAMPLE_SPECIFIC_INT_TAGS_SQL = 
			SAMPLE_INT_TAG_SQL + "AND sample_int_fields.field_id = ANY(?::int[])";

	private static final String SAMPLE_FLOAT_TAG_SQL = 
			"SELECT sample_float_fields.id, sample_float_fields.field_id, sample_float_fields.value FROM sample_float_fields WHERE sample_float_fields.sample_id = ?";

	public final static String SAMPLE_PERSONS_SQL = 
			"SELECT sample_persons.id, sample_persons.person_id FROM sample_persons WHERE sample_persons.sample_id = ?";

	public final static String SAMPLE_PERSON_IDS_SQL = 
			"SELECT sample_persons.person_id FROM sample_persons WHERE sample_persons.sample_id = ?";

	
	private static final String TAGS_SQL = 
			"SELECT fields.id, fields.name FROM fields WHERE fields.id = ANY(?::int[])";

	private static final String SAMPLE_EXPERIMENT_SQL = 
			"SELECT samples.experiment_id FROM samples WHERE samples.id = ?";

	private static final Set<String> ALL_VIEW = 
			CollectionUtils.asSet("all");

	private static final Set<Integer> ALL_TAGS = Collections.emptySet();

	private DataSource ds = null;

	public DatabaseServlet() {
		try {
			ds = (DataSource)Application.lookup("jdbc/experimentdb");
		} catch (NamingException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Returns the current version of the ExperimentDB database. This is 
	 * user adjustable in the database so that tools know if there have
	 * been updates etc. It is the int of the timestamp since epoch.
	 * 
	 * @param connection
	 * @return
	 * @throws SQLException
	 * @throws SQLException 
	 * @throws ParseException 
	 */
	public static int getVersion(Connection connection) throws SQLException, ParseException {
		int version = -1;

		PreparedStatement statement = connection.prepareStatement(VERSION_SQL);

		try {
			DatabaseResultsTable table = JDBCConnection.getTable(statement);

			version = (int)table.getDataAsDouble(0, 1);
		} finally {
			statement.close();
		}

		return version;
	}

	/**
	 * Returns the public id of an experiment.
	 * 
	 * @param context
	 * @param connection
	 * @param experimentId
	 * @return
	 * @throws SQLException
	 * @throws SQLException 
	 */
	protected static String getExperimentPublicId(Connection connection, 
			int experimentId) throws SQLException {

		//ConcurrentIdTextStore map = 
		//		(ConcurrentIdTextStore)context.getAttribute(Application.PUBLIC_ID_STORE_ATTRIBUTE);

		//if (map.contains(experimentId)) {
		//	return map.get(experimentId);
		//}

		PreparedStatement statement = 
				connection.prepareStatement(EXPERIMENT_PUBLIC_ID_SQL);

		String ret = null;

		try {
			statement.setInt(1, experimentId);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Returns the experiment id for a given sample.
	 * 
	 * @param context
	 * @param connection
	 * @param sampleId
	 * @return
	 * @throws SQLException
	 */
	protected static int getExperimentId(Connection connection, 
			int sampleId) throws SQLException {
		Cache cache = CacheManager.getInstance().getCache("sample-experiment-cache");

		Element ce = cache.get(sampleId);

		if (ce != null) {
			return (int)ce.getObjectValue();
		}

		int experimentId = -1;

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_EXPERIMENT_SQL);

		try {
			statement.setInt(1, sampleId);

			experimentId = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		cache.put(new Element(sampleId, experimentId));

		return experimentId;
	}

	/**
	 * Returns text tags associated with an entity.
	 * 
	 * @param connection
	 * @param sampleId
	 * @return
	 * @throws SQLException
	 */
	protected static DatabaseResultsTable getTextTagsTable(Connection connection, 
			int sampleId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_TAG_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);

			//System.err.println(statement);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static DatabaseResultsTable getTextTagsTable(Connection connection, 
			int sampleId,
			Collection<Integer> tags) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_SPECIFIC_TAGS_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);
			statement.setArray(2, Database.createConnArray(connection, tags));

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static DatabaseResultsTable getIntTagsTable(Connection connection, 
			int sampleId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_INT_TAG_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static DatabaseResultsTable getIntTagsTable(Connection connection, 
			int sampleId,
			Collection<Integer> tags) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_SPECIFIC_INT_TAGS_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);
			statement.setArray(2, Database.createConnArray(connection, tags));
			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static DatabaseResultsTable getFloatTagsTable(Connection connection, 
			int sampleId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SAMPLE_FLOAT_TAG_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static DatabaseResultsTable getSampleFilesTable(Connection connection,
			ServletContext context,
			int userId,
			int sampleId) throws SQLException, ParseException {

		//boolean isAdmin = 
		//		WebAuthentication.getIsAdminOrSuper(connection, userId);

		return Vfs.getSampleFilesTable(connection, sampleId);
	}

	protected static DatabaseResultsTable getPersonTable(Connection connection, 
			int sampleId) throws SQLException {

		return JDBCConnection.getTable(connection, SAMPLE_PERSONS_SQL, sampleId);
	}
	
	protected static List<Integer> getPersonIds(Connection connection, 
			int sampleId) throws SQLException {

		return JDBCConnection.getIntList(connection, SAMPLE_PERSON_IDS_SQL, sampleId);
	}

	public static void getGeo(Connection connection, 
			int sampleId, 
			JsonBuilder sampleJSON) throws SQLException {
		String json = getSampleGeoJson(connection, sampleId);

		if (json != null) {
			//cache.put(new Element(sampleId, json));

			sampleJSON.insert("geo", json);
		} 
		//else {
		//	sampleJSON.add("geo", constructGeoJson(connection, sampleId));
		//}
	}

	public static JsonObject constructGeoJson(Connection connection, 
			int sampleId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(Database.SAMPLE_GEO_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		if (table != null && table.getRowCount() > 0) {
			JsonObject geoJSON = new JsonObject();

			geoJSON.add(Application.HEADING_ID, 
					table.getDataAsInt(0, 0));
			geoJSON.add("geo_series_accession", 
					table.getDataAsString(0, 1));
			geoJSON.add("geo_accession", 
					table.getDataAsString(0, 2));
			geoJSON.add("geo_platform", 
					table.getDataAsString(0, 3));

			//sampleJSON.add("geo", geoJSON);

			return geoJSON;
		} else {
			return null;
		}
	}

	protected static String getSampleGeoJson(Connection connection, 
			int sampleId) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(JSON_SAMPLE_GEO_SQL);

		String ret = null;

		try {
			statement.setInt(1, sampleId);

			//System.err.println("statement " + ret);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Returns the sample ids associated with an experiment.
	 * 
	 * @param context
	 * @param connection
	 * @param experimentId
	 * @return
	 * @throws SQLException
	 * @throws SQLException
	 */
	protected static List<Integer> getSampleIds(ServletContext context, 
			Connection connection, 
			int experimentId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(EXPERIMENT_SAMPLES_SQL);

		List<Integer> ids = new ArrayList<Integer>();

		try {
			statement.setInt(1, experimentId);

			ids.addAll(JDBCConnection.getIntList(statement));

		} finally {
			statement.close();
		}

		return ids;
	}

	protected static DatabaseResultsTable getSample(Connection connection, 
			int sampleId) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(Database.SAMPLE_SQL);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, sampleId);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static void processSamples(Connection connection,
			int userId,
			DatabaseResultsTable table, 
			JsonBuilder jsonArray) throws SQLException, ParseException {
		processSamples(connection,
				userId,
				ALL_TAGS,
				table,
				jsonArray);
	}

	protected static void processSamples(Connection connection,
			int userId,
			final Set<Integer> tags,
			DatabaseResultsTable table, 
			JsonBuilder jsonArray) throws SQLException, ParseException {
		processSamples(connection,
				userId,
				tags,
				ALL_VIEW,
				table,
				jsonArray);
	}

	protected static void processSamples(Connection connection,
			int userId,
			Set<Integer> tags,
			Set<String> views,
			DatabaseResultsTable table,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		boolean isAdmin = 
				WebAuthentication.getIsAdminOrSuper(connection, userId);

		// output test values
		//JsonObject testJSON = new JsonObject();
		//testJSON.add("admin", isAdmin);
		//testJSON.add("size", table.getRowCount());
		//jsonArray.add(testJSON);

		views = TextUtils.toLowerCase(views);

		boolean tagView = 
				views.contains("all") || views.contains("tags");

		boolean personView = 
				views.contains("all") || views.contains("persons");

		boolean geoView = 
				views.contains("all") || views.contains("geo");

		boolean filesView = 
				views.contains("all") || views.contains("files");

		for (int i = 0; i < table.getRowCount(); ++i) {
			int sampleId = table.getDataAsInt(i, 0);
			int experimentId = table.getDataAsInt(i, 1);

			// If you're not an admin, restrict which samples you can see.
			boolean isLocked = !WebAuthentication.canViewSample(connection, 
					experimentId, 
					sampleId, 
					userId, 
					isAdmin);

			jsonArray.startObject();

			//if (views.contains("all") || views.contains("samples")) {
			jsonArray.add(Application.HEADING_ID, 
					sampleId);
			jsonArray.add(Application.HEADING_EXPERIMENT_ID, 
					experimentId);
			jsonArray.add(Application.HEADING_TYPE, 
					table.getDataAsInt(i, 2));
			jsonArray.add(Application.HEADING_NAME, 
					table.getDataAsString(i, 3));
			//sampleJSON.add(Application.HEADING_DESCRIPTION, table.getDataAsString(i, 4));
			jsonArray.add(Application.HEADING_SPECIES, 
					table.getDataAsInt(i, 4));
			jsonArray.add(Application.HEADING_RELEASED, 
					table.getDataAsString(i, 5));
			jsonArray.add(Application.HEADING_STATE, 
					SampleState.shortCode(SampleState.parse(isLocked)));

			//}

			if (tagView) {
				//jsonArray.add(Application.HEADING_TAGS, 
				//		getTagsJson(connection, sampleId, tags));
				
				jsonArray.startArray(Application.HEADING_TAGS);
				getTagsJson(connection, sampleId, tags, jsonArray);
				jsonArray.endArray();
			}

			///if (personView) {
			//	getPersonsJson(connection, sampleId, sampleJSON);
			//}

			if (geoView) {
				getGeo(connection, sampleId, jsonArray);
			}

			/*
			if (filesView) {
				DatabaseResultsTable filesTable = 
						Vfs.getSampleFilesTable(connection, sampleId);

				JsonArray filesJSON = new JsonArray();

				processFiles(connection, filesTable, filesJSON);

				sampleJSON.add(Application.HEADING_FILES, filesJSON);
			}
			 */

			jsonArray.endObject();
		}
	}

	private static void getPersonsJson(Connection connection, 
			int sampleId,
			JsonBuilder sampleJSON) throws SQLException {
		
		String json = getSamplePersonsJson(connection, sampleId);

		if (json != null) {
			//cache.put(new Element(sampleId, json));

			sampleJSON.insert("persons", json);
		} 
		//else {
		//	sampleJSON.add("geo", constructPersonsJson(connection, sampleId));
		//}
	}
	
	protected static String getSamplePersonsJson(Connection connection, 
			int sampleId) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(JSON_SAMPLE_PERSONS_SQL);

		String ret = null;

		try {
			statement.setInt(1, sampleId);

			//System.err.println("statement " + ret);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	public static JsonArray constructPersonsJson(Connection connection, 
			int sampleId) throws SQLException {
		List<Integer> ids = 
				getPersonIds(connection, sampleId);

		JsonArray personsJSON = new JsonArray();

		for (int id : ids) {
			//JsonObject personJSON = new JsonObject();

			//personJSON.add(Application.HEADING_ID, 
			//		personTable.getDataAsInt(j, 1));
			//fileJSON.add(Application.HEADING_NAME, filestable.getDataAsString(j, 1));
			//fileJSON.add(Application.HEADING_TYPE_ID, new JsonString(filestable.getDataAsString(j, 2)));

			personsJSON.add(id);
		}

		return personsJSON;
	}
	
	/**
	 * List persons associated with samples.
	 * 
	 * @param connection
	 * @param userId
	 * @param sampleIds
	 * @param jsonArray
	 * @throws SQLException
	 * @throws ParseException
	 */
	protected static void processSamplesPersons(Connection connection,
			int userId,
			Collection<Integer> sampleIds,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		for (int sampleId : sampleIds) {
			//JsonObject sampleJSON = JsonObject.create();
			jsonArray.startObject();
			
			//if (views.contains("all") || views.contains("samples")) {
			jsonArray.add(Application.HEADING_ID, sampleId);

			getPersonsJson(connection, sampleId, jsonArray);

			// For testing only
			//sampleJSON.add("admin", isAdmin);
			//sampleJSON.add("can_view", WebAuthentication.canViewSample(connection, 
			//		context,
			//		sampleId, 
			//		userId));
			//sampleJSON.add("s", sampleId);
			//sampleJSON.add("can_view", userId);

			jsonArray.endObject(); ////jsonArray.add(sampleJSON);
		}
	}

	protected static void processSample(Connection connection,
			ServletContext context,
			int userId,
			Set<Integer> tags,
			Set<String> views,
			DatabaseResultsTable table,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		//boolean isAdmin = 
		//		WebAuthentication.getIsAdminOrSuper(connection, userId);

		// output test values
		//JsonObject testJSON = new JsonObject();
		//testJSON.add("admin", isAdmin);
		//testJSON.add("size", table.getRowCount());
		//jsonArray.add(testJSON);

		views = TextUtils.toLowerCase(views);

		boolean tagView = 
				views.contains("all") || views.contains("tags");

		boolean personView = 
				views.contains("all") || views.contains("persons");

		boolean geoView = 
				views.contains("all") || views.contains("geo");

		boolean filesView = 
				views.contains("all") || views.contains("files");

		for (int i = 0; i < table.getRowCount(); ++i) {
			int sampleId = table.getDataAsInt(i, 0);
			int experimentId = table.getDataAsInt(i, 1);

			// If you're not an admin, restrict which samples you can see.
			//boolean isLocked = !WebAuthentication.canViewSample(connection, 
			//		context, 
			//		experimentId, 
			//		sampleId, 
			//		userId, 
			//		isAdmin);

			jsonArray.startObject();
			
			//JsonObject sampleJSON = new JsonObject();
			
			//if (views.contains("all") || views.contains("samples")) {
			jsonArray.add(Application.HEADING_ID, 
					sampleId);
			jsonArray.add(Application.HEADING_EXPERIMENT_ID, 
					experimentId);
			jsonArray.add(Application.HEADING_TYPE, 
					table.getDataAsInt(i, 2));
			jsonArray.add(Application.HEADING_NAME, 
					table.getDataAsString(i, 3));
			//sampleJSON.add(Application.HEADING_DESCRIPTION, table.getDataAsString(i, 4));
			jsonArray.add(Application.HEADING_SPECIES, 
					table.getDataAsInt(i, 4));
			jsonArray.add(Application.HEADING_RELEASED, 
					table.getDataAsString(i, 5));
			//sampleJSON.add(Application.HEADING_STATE, 
			//		SampleState.shortCode(SampleState.parse(isLocked)));

			//}

			if (tagView) {
				jsonArray.add(Application.HEADING_TAGS, 
						getTagsJson(connection, sampleId, tags));
			}

			if (personView) {
				DatabaseResultsTable personTable = 
						getPersonTable(connection, sampleId);

				//JsonArray personsJSON = new JsonArray();
				
				jsonArray.startArray("persons");

				for (int j = 0; j < personTable.getRowCount(); ++j) {
					//JsonObject personJSON = new JsonObject();
					//jsonArray.startObject("person");
					
					jsonArray.add(personTable.getDataAsInt(j, 1));
					//fileJSON.add(Application.HEADING_NAME, filestable.getDataAsString(j, 1));
					//fileJSON.add(Application.HEADING_TYPE_ID, new JsonString(filestable.getDataAsString(j, 2)));

					//jsonArray.add(personJSON);
					
					//jsonArray.endObject();
				}

				//sampleJson.add("persons", personsJSON);
				jsonArray.endArray();
			}

			if (geoView) {
				getGeo(connection, sampleId, jsonArray);
			}

			/*
			if (filesView) {
				DatabaseResultsTable filesTable = 
						Vfs.getSampleFilesTable(connection, sampleId);

				JsonArray filesJSON = new JsonArray();

				processFiles(connection, filesTable, filesJSON);

				sampleJSON.add(Application.HEADING_FILES, filesJSON);
			}
			 */

			jsonArray.endObject();
		}
	}

	/**
	 * Return the state associated with a list of samples for a given user.
	 * 
	 * @param connection
	 * @param context
	 * @param userId
	 * @param sampleIds
	 * @param jsonArray
	 * @throws SQLException
	 * @throws ParseException
	 */
	protected static void processSampleStates(Connection connection,
			ServletContext context,
			int userId,
			Collection<Integer> sampleIds,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		boolean isAdmin = 
				WebAuthentication.getIsAdminOrSuper(connection, userId);

		for (int sampleId : sampleIds) {
			boolean isLocked = !isAdmin && 
					!WebAuthentication.canViewSample(connection,
							sampleId, 
							userId);

			//JsonObject sampleJSON = JsonObject.create();
			jsonArray.startObject();

			//if (views.contains("all") || views.contains("samples")) {
			jsonArray.add(Application.HEADING_ID, 
					sampleId);

			jsonArray.add(Application.HEADING_STATE, 
					SampleState.shortCode(SampleState.parse(isLocked)));


			// For testing only
			//sampleJSON.add("admin", isAdmin);
			//sampleJSON.add("can_view", WebAuthentication.canViewSample(connection, 
			//		context,
			//		sampleId, 
			//		userId));
			//sampleJSON.add("s", sampleId);
			//sampleJSON.add("can_view", userId);

			jsonArray.endObject();
		}
	}

	/**
	 * Add all the tags for a sample to the json array.
	 * 
	 * @param connection
	 * @param sampleId
	 * @param array
	 * @return 
	 * @return
	 * @throws SQLException
	 */
	public static String getTagsJson(Connection connection, 
			int sampleId) throws SQLException {
		return getTagsJson(connection, sampleId, ALL_TAGS);
	}
	
	public static void getTagsJson(Connection connection, 
			int sampleId,
			JsonBuilder jsonArray) throws SQLException {
		getTagsJson(connection, sampleId, ALL_TAGS, jsonArray);
	}
	
	protected static String getTagsJson(Connection connection, 
			int sampleId,
			final Set<Integer> tags) throws SQLException {
		JsonBuilder jsonArray = new JsonBuilder().startArray();
		
		getTagsJson(connection, sampleId, tags, jsonArray);
		
		jsonArray.endArray();
		
		return jsonArray.toString();
	}

	/**
	 * Returns the tags associated with a sample.
	 * 
	 * @param connection
	 * @param sampleId
	 * @param tags
	 * @param array
	 * @return 
	 * @throws SQLException
	 */
	protected static void getTagsJson(Connection connection, 
			int sampleId,
			final Set<Integer> tags,
			JsonBuilder jsonArray) throws SQLException {

		//Cache cache = CacheManager.getInstance().getCache("sample-fields-cache");

		//Element ce = cache.get(sampleId);

		// Item is in the cache so reuse
		//if (ce != null) {
		//	array.setJson((String)ce.getObjectValue());
		//
		//	return;
		//}

		// First lets see if we can use the json cache before constructing
		// the tags with expensive DB operations
		String json = getSampleFieldsJson(connection, sampleId);

		if (json != null) {
			// Clip the array brackets off the string since we don't need
			// them
			jsonArray.insert(json.substring(1, json.length() - 1));
		}

		//JsonBuilder array = new JsonBuilder().startArray();
		
		constructTagsJson(connection, sampleId, jsonArray);
		
		//array.endArray();
	}


	/**
	 * 
	 * @param connection
	 * @param sampleId
	 * @param array
	 * @throws SQLException
	 */
	public static void constructTagsJson(Connection connection, 
			int sampleId,
			JsonBuilder array) throws SQLException {

		DatabaseResultsTable tagTable = getTextTagsTable(connection, sampleId);

		for (int j = 0; j < tagTable.getRowCount(); ++j) {
			array.startObject();

			array.add(Application.HEADING_ID, 
					tagTable.getDataAsInt(j, 1));
			//tagJson.add(Application.HEADING_TAG_ID, 
			//		tagTable.getDataAsInt(j, 1));
			array.add(Application.HEADING_VALUE, 
					tagTable.getDataAsString(j, 2));

			array.endObject();
		}

		tagTable = getIntTagsTable(connection, sampleId);

		for (int j = 0; j < tagTable.getRowCount(); ++j) {
			array.startObject();

			array.add(Application.HEADING_ID, 
					tagTable.getDataAsInt(j, 1));
			
			//tagJson.add(Application.HEADING_TAG_ID, 
			//		tagTable.getDataAsInt(j, 1));
			
			array.add(Application.HEADING_VALUE, 
					tagTable.getDataAsInt(j, 2));

			array.endObject();
		}

		tagTable = getFloatTagsTable(connection, sampleId);

		for (int j = 0; j < tagTable.getRowCount(); ++j) {
			array.startObject();

			array.add(Application.HEADING_ID, tagTable.getDataAsInt(j, 1));
			//tagJson.add(Application.HEADING_TAG_ID, tagTable.getDataAsInt(j, 1));
			array.add(Application.HEADING_VALUE, tagTable.getDataAsDouble(j, 2));

			array.endObject();
		}
	}

	/**
	 * Returns the json available in the cached table.
	 * 
	 * @param connection
	 * @param sampleId
	 * @param array
	 * @return 
	 * @throws SQLException
	 */
	protected static String getSampleFieldsJson(Connection connection, 
			int sampleId) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(JSON_SAMPLE_FIELDS_SQL);

		String ret = null;

		try {
			statement.setInt(1, sampleId);

			//System.err.println("statement " + ret);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		//System.err.println("ret " + ret);

		//if (ret != null) {
		//	array.insert(ret);
		//}

		return ret;
	}

	protected static void processSamples(Connection connection,
			DatabaseResultsTable table, 
			JsonBuilder jsonArray) throws SQLException, ParseException {
		if (table == null) {
			return;
		}

		for (int i = 0; i < table.getRowCount(); ++i) {
			int sampleId = table.getDataAsInt(i, 0);

			//JsonObject sampleJSON = new JsonObject();
			jsonArray.startObject();
			
			jsonArray.add(Application.HEADING_ID, sampleId);
			jsonArray.add(Application.HEADING_EXPERIMENT_ID, table.getDataAsInt(i, 1));
			jsonArray.add(Application.HEADING_EXPRESSION_TYPE_ID, table.getDataAsInt(i, 2));
			jsonArray.add(Application.HEADING_NAME, table.getDataAsString(i, 3));
			jsonArray.add(Application.HEADING_DESCRIPTION, table.getDataAsString(i, 4));
			jsonArray.add(Application.HEADING_SPECIES, table.getDataAsInt(i, 5));
			jsonArray.add(Application.HEADING_RELEASED, table.getDataAsString(i, 6));

			//JsonArray tagsJSON = new JsonArray();
			jsonArray.startObject(Application.HEADING_TAGS);
			
			DatabaseResultsTable fieldTable = getTextTagsTable(connection, sampleId);

			for (int j = 0; j < fieldTable.getRowCount(); ++j) {
				//JsonObject fieldJSON = new JsonObject();
				jsonArray.startObject();

				jsonArray.add(Application.HEADING_ID, fieldTable.getDataAsInt(j, 0));
				jsonArray.add(Application.HEADING_TAG_ID, fieldTable.getDataAsInt(j, 1));
				jsonArray.add(Application.HEADING_VALUE, fieldTable.getDataAsString(j, 2));

				jsonArray.endObject();
			}

			fieldTable = getIntTagsTable(connection, sampleId);

			for (int j = 0; j < fieldTable.getRowCount(); ++j) {
				//JsonObject fieldJSON = new JsonObject();
				jsonArray.startObject();
				
				jsonArray.add(Application.HEADING_ID, fieldTable.getDataAsInt(j, 0));
				jsonArray.add(Application.HEADING_TAG_ID, fieldTable.getDataAsInt(j, 1));
				jsonArray.add(Application.HEADING_VALUE, fieldTable.getDataAsInt(j, 2));

				jsonArray.endObject();
			}

			fieldTable = getFloatTagsTable(connection, sampleId);

			for (int j = 0; j < fieldTable.getRowCount(); ++j) {
				//JsonObject fieldJSON = new JsonObject();
				jsonArray.startObject();
				
				jsonArray.add(Application.HEADING_ID, fieldTable.getDataAsInt(j, 0));
				jsonArray.add(Application.HEADING_TAG_ID, fieldTable.getDataAsInt(j, 1));
				jsonArray.add(Application.HEADING_VALUE, fieldTable.getDataAsDouble(j, 2));

				jsonArray.endObject();
			}

			jsonArray.endObject();

			DatabaseResultsTable personTable = 
					getPersonTable(connection, sampleId);

			jsonArray.startArray("persons");

			for (int j = 0; j < personTable.getRowCount(); ++j) {
				//JsonObject personJSON = new JsonObject();

				jsonArray.add(Application.HEADING_ID, personTable.getDataAsInt(j, 1));
				//fileJSON.add(Application.HEADING_NAME, filestable.getDataAsString(j, 1));
				//fileJSON.add(Application.HEADING_TYPE_ID, new JsonString(filestable.getDataAsString(j, 2)));

				//personsJSON.add(personJSON);
			}

			jsonArray.endArray();

			getGeo(connection, sampleId, jsonArray);

			DatabaseResultsTable filesTable = 
					Vfs.getSampleFilesTable(connection, sampleId);

			jsonArray.startArray(Application.HEADING_FILES);

			for (int j = 0; j < filesTable.getRowCount(); ++j) {
				//JsonObject fileJSON = new JsonObject();
				jsonArray.startObject();
				
				jsonArray.add(Application.HEADING_ID, filesTable.getDataAsInt(j, 0));
				jsonArray.add(Application.HEADING_NAME, filesTable.getDataAsString(j, 1));
				jsonArray.add(Application.HEADING_TYPE_ID, filesTable.getDataAsInt(j, 2));

				jsonArray.endObject();
			}

			jsonArray.endArray();
		}
	}

	protected static void processExperiments(Connection connection,
			int personId,
			DatabaseResultsTable table,
			JsonBuilder jsonArray) throws SQLException, ParseException {
		processExperiments(connection,
				personId,
				table,
				ALL_VIEW,
				jsonArray);
	}

	protected static void processExperiments(Connection connection,
			int personId,
			DatabaseResultsTable table,
			Set<String> views,
			JsonBuilder jsonArray) throws SQLException {

		for (int i = 0; i < table.getRowCount(); ++i) {
			int experimentId = table.getDataAsInt(i, 0);

			boolean isLocked = !WebAuthentication.canViewExperiment(connection,
					experimentId, 
					personId);

			jsonArray.startObject(); //JsonObject json = new JsonObject();

			jsonArray.add(Application.HEADING_ID, experimentId);
			jsonArray.add(Application.HEADING_PUBLIC_ID, table.getDataAsString(i, 1));
			jsonArray.add(Application.HEADING_NAME, table.getDataAsString(i, 2));
			jsonArray.add(Application.HEADING_DESCRIPTION, table.getDataAsString(i, 3));
			jsonArray.add(Application.HEADING_RELEASED, table.getDataAsString(i, 4));
			jsonArray.add(Application.HEADING_LOCKED, isLocked);

			jsonArray.endObject(); //jsonArray.add(json);
		}
	}

	protected static void processTypes(Connection connection,
			List<Type> types,
			JsonArray jsonArray) {

		for (Type type : types) {
			JsonObject fieldJSON = new JsonObject();

			//attributesJSON.add(Application.HEADING_ID, new JSONInteger(table.getDataAsInt(i, 0)));
			fieldJSON.add(Application.HEADING_ID, type.getId());
			fieldJSON.add(Application.HEADING_NAME, type.getName());

			jsonArray.add(fieldJSON);
		}
	}

	protected static void processTypes(DatabaseResultsTable table,
			JsonBuilder jsonArray) {

		for (int i = 0; i < table.getRowCount(); ++i) {
			//JsonObject fieldJSON = new JsonObject();
			jsonArray.startObject();
			
			//attributesJSON.add(Application.HEADING_ID, new JSONInteger(table.getDataAsInt(i, 0)));
			jsonArray.add(Application.HEADING_ID, table.getDataAsInt(i, 0));
			jsonArray.add(Application.HEADING_NAME, table.getDataAsString(i, 1));

			jsonArray.endObject();
		}
	}

	protected static void processSampleFiles(Connection connection,
			int sampleId,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		List<Integer> ids = Vfs.getSampleFiles(connection, sampleId);

		DatabaseResultsTable table = Vfs.getFilesTable(connection, ids);

		processFiles(connection, table, jsonArray);
	}

	protected static void processExperimentFiles(Connection connection,
			int experimentId,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		DatabaseResultsTable table = 
				Vfs.getExperimentFilesTable(connection, experimentId);

		processFiles(connection, table, jsonArray);
	}

	protected static void processExperimentFilesDir(Connection connection,
			int experimentId,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		DatabaseResultsTable table = 
				Vfs.getExperimentFilesDirTable(connection, experimentId);

		processFiles(connection, table, jsonArray);
	}

	protected static void processFiles(Connection connection,
			DatabaseResultsTable table, 
			JsonBuilder jsonArray) throws ParseException, SQLException {
		for (int i = 0; i < table.getRowCount(); ++i) {
			int vfsId = table.getDataAsInt(i, 0);

			//Json fileJson = new JsonObject();
			jsonArray.startObject();
			
			jsonArray.add(Application.HEADING_ID, vfsId);
			jsonArray.add("pid", table.getDataAsInt(i, 1));
			jsonArray.add(Application.HEADING_NAME, table.getDataAsString(i, 2));
			jsonArray.add(Application.HEADING_TYPE, table.getDataAsInt(i, 3));
			jsonArray.add(Application.HEADING_CREATED, table.getDataAsString(i, 5));

			//JsonArray tagsJson = new JsonArray();
			//processVfsTags(connection, vfsId, tagsJson);
			//fileJson.add(Application.HEADING_TAGS, tagsJson);

			jsonArray.endObject();
		}
	}


	/**
	 * Return the info for a particular file.
	 * 
	 * @param connection
	 * @param vfsId
	 * @param jsonArray
	 * @throws ParseException
	 * @throws SQLException
	 */
	protected static void processFile(Connection connection,
			int vfsId,
			JsonBuilder jsonArray) throws ParseException, SQLException {
		DatabaseResultsTable table = Vfs.getFileTable(connection, vfsId);

		processFiles(connection, table, jsonArray);
	}

	protected static void processFiles(Connection connection,
			ServletContext context,
			int personId,
			DatabaseResultsTable table, 
			JsonArray jsonArray) throws ParseException, SQLException {
		for (int i = 0; i < table.getRowCount(); ++i) {
			int vfsId = table.getDataAsInt(i, 0);

			if (WebAuthentication.getCanViewFile(connection, context, personId, vfsId)) {
				JsonObject fileJSON = new JsonObject();

				fileJSON.add(Application.HEADING_ID, vfsId);
				fileJSON.add("pid", table.getDataAsInt(i, 1));
				fileJSON.add(Application.HEADING_NAME, table.getDataAsString(i, 2));
				fileJSON.add(Application.HEADING_FILE_TYPE_ID, table.getDataAsInt(i, 3));

				jsonArray.add(fileJSON);
			}
		}
	}

	protected static void processVfsTags(Connection connection,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		List<Integer> ids = Vfs.getVfsTags(connection);

		DatabaseResultsTable table = getTagsTable(connection, ids);

		processTags(table, jsonArray);
	}

	protected static void processVfsTags(Connection connection,
			int vfsId,
			JsonBuilder jsonArray) throws SQLException, ParseException {

		List<Integer> ids = Vfs.getVfsTags(connection, vfsId);

		DatabaseResultsTable table = getTagsTable(connection, ids);

		processTags(table, jsonArray);
	}

	protected static DatabaseResultsTable getTagsTable(Connection connection, 
			List<Integer> ids) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(TAGS_SQL);

		Array array = Database.createConnArray(connection, ids);

		DatabaseResultsTable table = null;

		try {
			statement.setArray(1, array);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	protected static void processTags(DatabaseResultsTable table, 
			JsonBuilder jsonArray) throws ParseException {
		processTypes(table, jsonArray);
	}

	/**
	 * Returns the attribute name for a given attribute id.
	 * This method takes care of caching results to reduce database access.
	 * 
	 * @param context
	 * @param connection
	 * @param attributeId
	 * @return
	 * @throws SQLException
	 * @throws SQLException 
	 */
	/*
	protected String getAttribute(ServletContext context, 
			Connection connection, 
			int attributeId) throws SQLException {

		ConcurrentIdTextStore map = 
				(ConcurrentIdTextStore)context.getAttribute(Application.ATTRIBUTES_STORE_ATTRIBUTE);

		if (map.contains(attributeId)) {
			return map.get(attributeId);
		}

		PreparedStatement statement = connection.prepareStatement(ATTRIBUTE_SQL);

		String ret = null;

		try {
			statement.setInt(1, attributeId);

			DatabaseResultsTable table = JDBCConnection.getTable(statement);

			ret = table.getRowCount() == 1 ? table.getDataAsString(0, 0) : null;
		} finally {
			statement.close();
		}

		if (ret != null) {
			map.put(attributeId, ret);
		}

		return ret;
	}
	 */

	protected static int getTagId(Connection connection, 
			Path path) throws SQLException {
		return getTagId(connection, path.toString());
	}

	/**
	 * Returns the search field id for a keyword, or
	 * -1 if the keyword does not exist in the database.
	 * 
	 * @param connection
	 * @param name
	 * @return
	 * @throws SQLException
	 */
	protected static int getTagId(Connection connection, 
			String name) throws SQLException {

		PreparedStatement statement = connection.prepareStatement(TAG_ID_SQL);

		int id = -1;

		try {
			statement.setString(1, name);

			id = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		return id;
	}

	protected static int getSampleIdFromGeoAccession(Connection connection, 
			String accession) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(GEO_SAMPLE_SQL);

		int id = -1;

		try {
			statement.setString(1, accession);

			id = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		return id;
	}

	/**
	 * Returns a pooled database connection.
	 * 
	 * @return
	 * @throws SQLException 
	 */
	protected Connection getConnection() throws SQLException {
		if (ds == null) {
			return null;
		}

		return ds.getConnection();
	}

	/**
	 * Parses a string as a number returning -1
	 * if the string is a badly formed number.
	 * 
	 * @param id
	 * @return
	 */
	public int parseId(String id) {
		int ret = -1;

		try {
			ret = Integer.parseInt(id);
		} catch (Exception e) {
			// do noting
		}

		return ret;
	}

	protected int authenticateKey(ServletContext context, 
			HttpServletRequest request, 
			Connection connection, 
			String key) {
		return 6;
	}

	public static void processSamples(Connection connection,
			ServletContext context,
			Person person,
			DatabaseResultsTable table,
			Collection<Sample> samples,
			Map<Integer, Species> organisms,
			Map<Integer, Experiment> experiments,
			TypeMap expressionTypes) throws SQLException, ParseException {
		processSamples(connection, 
				context,
				person.getId(), 
				table, 
				samples,
				organisms,
				experiments,
				expressionTypes);
	}

	public static void processSamples(Connection connection,
			ServletContext context,
			int userId,
			DatabaseResultsTable table,
			Collection<Sample> samples,
			Map<Integer, Species> organisms,
			Map<Integer, Experiment> experiments,
			TypeMap expressionTypes) throws SQLException, ParseException {

		//Map<Integer, Organism> organisms = Database.getOrganisms(connection);
		//Map<Integer, Experiment> experiments = Database.getExperiments(connection);
		//Map<Integer, Type> expressionTypes = Database.getExpressionTypes(connection);

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

		for (int i = 0; i < table.getRowCount(); ++i) {
			int sampleId = table.getDataAsInt(i, 0);

			if(!WebAuthentication.canViewSample(connection, sampleId, userId)) {
				continue;
			}

			Sample sample = new Sample(sampleId, 
					experiments.get(table.getDataAsInt(i, 1)),
					expressionTypes.get(table.getDataAsInt(i, 2)),
					table.getDataAsString(i, 3),
					organisms.get(table.getDataAsInt(i, 4)),
					formatter.parse(table.getDataAsString(i, 5)));

			samples.add(sample);
		}
	}

	/*
	protected static List<FileRecord> getFiles(Connection connection,
			int personId,
			List<Integer> ids,
			File dataDirectory) throws SQLException, IOException, SQLException {	

		List<FileRecord> files = new ArrayList<FileRecord>();

		for (Integer id : ids) {
			FileRecord file = getFile(connection, 
					personId, 
					id, 
					dataDirectory);

			if (file == null) {
				continue;
			}

			files.add(file);
		}

		return files;
	}

	protected static FileRecord getFile(Connection connection,
			int personId,
			int fileId,
			File dataDirectory) throws SQLException  {	

		FileRecord fileRecord = null;

		PreparedStatement statement = connection.prepareStatement(FILE_SQL);

		try {
			statement.setInt(1, fileId);

			DatabaseResultsTable table = JDBCConnection.getTable(statement);

			if (table.getRowCount() == 0) {
				return null;
			}

			int sampleId = table.getDataAsInt(0, 1);

			if (!Authentication.getCanViewSample(connection, sampleId, personId)) {
				return null;
			}



			String name = table.getDataAsString(0, 2);

			String path = table.getDataAsString(0, 3);

			fileRecord = new FileRecord(fileId, name, path);
		} finally {
			statement.close();
		}

		return fileRecord;
	}
	 */
}
