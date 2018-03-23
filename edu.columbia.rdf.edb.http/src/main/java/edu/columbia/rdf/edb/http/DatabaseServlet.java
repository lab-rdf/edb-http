/**
 * Copyright 2017 Antony Holmes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.jebtk.bioinformatics.annotation.Species;
import org.jebtk.bioinformatics.annotation.Type;
import org.jebtk.core.collections.CollectionUtils;
import org.jebtk.core.json.JsonArray;
import org.jebtk.core.json.JsonBuilder;
import org.jebtk.core.json.JsonObject;
import org.jebtk.core.path.Path;
import org.jebtk.core.text.TextUtils;
import org.jebtk.database.DatabaseResultsTable;
import org.jebtk.database.JDBCConnection;
import org.jebtk.database.ResultsSetTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import edu.columbia.rdf.edb.EDB;
import edu.columbia.rdf.edb.Experiment;
import edu.columbia.rdf.edb.Person;
import edu.columbia.rdf.edb.Sample;
import edu.columbia.rdf.edb.TypeMap;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

// TODO: Auto-generated Javadoc
/**
 * Provides a connection to the database for servlets and deals with closing the
 * connection.
 * 
 * @author Antony Holmes Holmes
 *
 */
public abstract class DatabaseServlet {
  // private Pattern KEY_PATTERN = Pattern.compile("^[\\w\\-]+$");

  /** The Constant EXPERIMENT_PUBLIC_ID_SQL. */
  private static final String EXPERIMENT_PUBLIC_ID_SQL = "SELECT experiments.public_id FROM experiments WHERE experiments.id = ?";

  /** The Constant TAG_ID_SQL. */
  private static final String TAG_ID_SQL = "SELECT tags.id FROM tags WHERE tags.name = ?";

  /** The Constant EXPERIMENT_SAMPLES_SQL. */
  private static final String EXPERIMENT_SAMPLES_SQL = "SELECT samples.id FROM samples WHERE samples.experiment_id = ?";

  /** The Constant VERSION_SQL. */
  private static final String VERSION_SQL = "SELECT version.id, EXTRACT(EPOCH FROM version.version) FROM version ORDER BY version.version DESC LIMIT 1";

  /** The Constant GEO_SAMPLE_SQL. */
  private static final String GEO_SAMPLE_SQL = "SELECT geo_samples.sample_id FROM geo_samples FROM geo_samples WHERE geo_samples.name = ?";

  /** The Constant TAGS_SAMPLE_SQL. */
  public static final String TAGS_SAMPLE_SQL = "SELECT tags_sample.id, tags_sample.tag_id, tags_sample.value FROM tags_sample WHERE tags_sample.sample_id = ?";

  /** Returns just the specific tags mentioned. */
  public static final String SAMPLE_SPECIFIC_TAG_SQL = TAGS_SAMPLE_SQL
      + " AND tags_sample.tag_id = ?";

  /** The Constant SAMPLE_SPECIFIC_TAGS_SQL. */
  private static final String SAMPLE_SPECIFIC_TAGS_SQL = TAGS_SAMPLE_SQL
      + " AND tags_sample.tag_id = ANY(?::int[])";

  /** The Constant TAGS_SAMPLE_INT_SQL. */
  public static final String TAGS_SAMPLE_INT_SQL = "SELECT tags_sample_int.id, tags_sample_int.tag_id, tags_sample_int.value FROM tags_sample_int WHERE tags_sample_int.sample_id = ?";

  /** The Constant SAMPLE_SPECIFIC_INT_TAG_SQL. */
  public static final String SAMPLE_SPECIFIC_INT_TAG_SQL = TAGS_SAMPLE_INT_SQL
      + " AND tags_sample_int.tag_id = ?";

  /** The Constant SAMPLE_SPECIFIC_INT_TAGS_SQL. */
  private static final String SAMPLE_SPECIFIC_INT_TAGS_SQL = TAGS_SAMPLE_INT_SQL
      + " AND tags_sample_int.tag_id = ANY(?::int[])";

  /** The Constant SAMPLE_FLOAT_TAG_SQL. */
  public static final String TAGS_SAMPLE_FLOAT_SQL = "SELECT tags_sample_float.id, tags_sample_float.tag_id, tags_sample_float.value FROM tags_sample_float WHERE tags_sample_float.sample_id = ?";

  /** The Constant SAMPLE_SPECIFIC_FLOAT_TAG_SQL. */
  public static final String SAMPLE_SPECIFIC_FLOAT_TAG_SQL = TAGS_SAMPLE_FLOAT_SQL
      + " AND tags_sample_float.tag_id = ?";

  /** The Constant SAMPLE_SPECIFIC_FLOAT_TAGS_SQL. */
  private static final String SAMPLE_SPECIFIC_FLOAT_TAGS_SQL = TAGS_SAMPLE_FLOAT_SQL
      + " AND tags_sample_float.tag_id = ANY(?::int[])";

  /** The Constant JSON_SAMPLE_FIELDS_SQL. */
  //private static final String JSON_SAMPLE_FIELDS_SQL = "SELECT json_sample_fields.json FROM json_sample_fields WHERE json_sample_fields.sample_id = ?";

  /** The Constant JSON_SAMPLE_GEO_SQL. */
  //private static final String JSON_SAMPLE_GEO_SQL = "SELECT json_sample_geo.json FROM json_sample_geo WHERE json_sample_geo.sample_id = ?";

  /** The Constant JSON_SAMPLE_PERSONS_SQL. */
  //private static final String JSON_SAMPLE_PERSONS_SQL = "SELECT json_sample_persons.json FROM json_sample_persons WHERE json_sample_persons.sample_id = ?";

  /** The Constant SAMPLE_PERSONS_SQL. */
  public final static String SAMPLE_PERSONS_SQL = "SELECT sample_persons.id, sample_persons.person_id FROM sample_persons WHERE sample_persons.sample_id = ?";

  /** The Constant SAMPLE_PERSON_IDS_SQL. */
  public final static String SAMPLE_PERSON_IDS_SQL = "SELECT sample_persons.person_id FROM sample_persons WHERE sample_persons.sample_id = ?";

  /** The Constant TAGS_SQL. */
  private static final String TAGS_SQL = "SELECT tags.id, tags.name FROM tags WHERE tags.id = ANY(?::int[])";

  /** The Constant SAMPLE_EXPERIMENT_SQL. */
  private static final String SAMPLE_EXPERIMENT_SQL = "SELECT samples.experiment_id FROM samples WHERE samples.id = ?";

  /** The Constant ALL_VIEW. */
  private static final Set<String> ALL_VIEW = CollectionUtils.asSet("all");

  /** The Constant ALL_TAGS. */
  private static final Set<Integer> ALL_TAGS = Collections.emptySet();

  /** The Constant ALL_ORGANISMS. */
  private static final Set<Integer> ALL_ORGANISMS = Collections.emptySet();

  /** The ds. */
  private DataSource mDs = null;

  @Autowired
  private JdbcTemplate mJdbcTemplate;

  /**
   * Instantiates a new database servlet.
   */
  public DatabaseServlet() {
    try {
      mDs = (DataSource) EDB.lookup("jdbc/experimentdb");
    } catch (NamingException e1) {
      e1.printStackTrace();
    }
  }

  /**
   * Returns the current version of the ExperimentDB database. This is user
   * adjustable in the database so that tools know if there have been updates
   * etc. It is the int of the timestamp since epoch.
   *
   * @param connection the connection
   * @return the version
   * @throws SQLException the SQL exception
   */
  public static int getVersion(Connection connection) throws SQLException {
    int version = -1;

    PreparedStatement statement = connection.prepareStatement(VERSION_SQL);

    try {
      DatabaseResultsTable table = JDBCConnection.getTable(statement);

      version = (int) table.getDataAsDouble(0, 1);
    } finally {
      statement.close();
    }

    return version;
  }

  /**
   * Returns the public id of an experiment.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the experiment public id
   * @throws SQLException the SQL exception
   */
  protected static String getExperimentPublicId(Connection connection,
      int experimentId) throws SQLException {

    // ConcurrentIdTextStore map =
    // (ConcurrentIdTextStore)context.getAttribute(Application.PUBLIC_ID_STORE_ATTRIBUTE);

    // if (map.contains(experimentId)) {
    // return map.get(experimentId);
    // }

    PreparedStatement statement = connection
        .prepareStatement(EXPERIMENT_PUBLIC_ID_SQL);

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
   * @param connection the connection
   * @param sampleId the sample id
   * @return the experiment id
   * @throws SQLException the SQL exception
   */
  protected static int getExperimentId(Connection connection, int sampleId)
      throws SQLException {
    Cache cache = CacheManager.getInstance()
        .getCache("sample-experiment-cache");

    Element ce = cache.get(sampleId);

    if (ce != null) {
      return (int) ce.getObjectValue();
    }

    int experimentId = -1;

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_EXPERIMENT_SQL);

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
   * @param connection the connection
   * @param sampleId the sample id
   * @return the text tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getTextTagsTable(Connection connection,
      int sampleId) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(TAGS_SAMPLE_SQL);

    statement.setInt(1, sampleId);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the text tags table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @return the text tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getTextTagsTable(Connection connection,
      int sampleId,
      Collection<Integer> tags) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_TAGS_SQL);

    statement.setInt(1, sampleId);
    statement.setArray(2, Database.createConnArray(connection, tags));

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the text tag table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tag the tag
   * @return the text tag table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getTextTagTable(Connection connection,
      int sampleId,
      int tag) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_TAG_SQL);

    statement.setInt(1, sampleId);
    statement.setInt(2, tag);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the int tags table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the int tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getIntTagsTable(Connection connection,
      int sampleId) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(TAGS_SAMPLE_INT_SQL);

    statement.setInt(1, sampleId);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the int tag table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tag the tag
   * @return the int tag table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getIntTagTable(Connection connection,
      int sampleId,
      int tag) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_INT_TAG_SQL);

    statement.setInt(1, sampleId);
    statement.setInt(2, tag);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the int tags table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @return the int tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getIntTagsTable(Connection connection,
      int sampleId,
      Collection<Integer> tags) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_INT_TAGS_SQL);

    statement.setInt(1, sampleId);
    statement.setArray(2, Database.createConnArray(connection, tags));

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the float tags table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the float tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getFloatTagsTable(Connection connection,
      int sampleId) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(TAGS_SAMPLE_FLOAT_SQL);

    statement.setInt(1, sampleId);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the float tag table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tag the tag
   * @return the float tag table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getFloatTagTable(Connection connection,
      int sampleId,
      int tag) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_FLOAT_TAG_SQL);

    statement.setInt(1, sampleId);
    statement.setInt(2, tag);

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the float tags table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @return the float tags table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getFloatTagsTable(Connection connection,
      int sampleId,
      Collection<Integer> tags) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(SAMPLE_SPECIFIC_FLOAT_TAGS_SQL);

    statement.setInt(1, sampleId);
    statement.setArray(2, Database.createConnArray(connection, tags));

    return JDBCConnection.resultSetTable(statement);
  }

  /**
   * Gets the sample files table.
   *
   * @param connection the connection
   * @param context the context
   * @param userId the user id
   * @param sampleId the sample id
   * @return the sample files table
   * @throws SQLException the SQL exception
   * @throws ParseException the parse exception
   */
  protected static ResultsSetTable getSampleFilesTable(Connection connection,
      ServletContext context,
      int userId,
      int sampleId) throws SQLException, ParseException {
    return Vfs.getSampleFilesTable(connection, sampleId);
  }

  /**
   * Gets the person table.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the person table
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getPersonTable(Connection connection,
      int sampleId) throws SQLException {
    return Database.getTable(connection, SAMPLE_PERSONS_SQL, sampleId);
  }

  /**
   * Gets the person ids.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the person ids
   * @throws SQLException the SQL exception
   */
  protected static List<Integer> getPersonIds(Connection connection,
      int sampleId) throws SQLException {
    return JDBCConnection
        .getIntList(connection, SAMPLE_PERSON_IDS_SQL, sampleId);
  }

  /**
   * Gets the geo.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param sampleJSON the sample JSON
   * @return the geo
   * @throws SQLException the SQL exception
   */
  public static void getGeo(Connection connection,
      int sampleId,
      JsonBuilder json) throws SQLException {
    /*
     * String json = getSampleGeoJson(connection, sampleId);
     * 
     * if (json != null) { sampleJSON.insert("geo", json); }
     */

    ResultsSetTable table = Database
        .getTable(connection, Database.SAMPLE_GEO_SQL, sampleId);

    if (table.next()) {
      json.startObject();
      json.add(EDB.HEADING_ID, table.getInt(0));
      json.add("geo_series_accession", table.getString(1));
      json.add("geo_accession", table.getString(2));
      json.add("geo_platform", table.getString(3));
      json.endObject();
    }
  }

  /**
   * Construct geo json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the json object
   * @throws SQLException the SQL exception
   */
  public static JsonObject constructGeoJson(Connection connection, int sampleId)
      throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(Database.SAMPLE_GEO_SQL);

    ResultsSetTable table = null;

    try {
      statement.setInt(1, sampleId);

      table = JDBCConnection.resultSetTable(statement);
    } finally {
      statement.close();
    }

    if (table != null) {
      JsonObject geoJSON = new JsonObject();

      while (table.next()) {
        geoJSON.add(EDB.HEADING_ID, table.getInt(0));
        geoJSON.add("geo_series_accession", table.getString(1));
        geoJSON.add("geo_accession", table.getString(2));
        geoJSON.add("geo_platform", table.getString(3));
      }

      return geoJSON;
    } else {
      return null;
    }
  }

  /*
  protected static String getSampleGeoJson(Connection connection, int sampleId)
      throws SQLException {
    PreparedStatement statement = connection
        .prepareStatement(JSON_SAMPLE_GEO_SQL);

    String ret = null;

    try {
      statement.setInt(1, sampleId);

      // System.err.println("statement " + ret);

      ret = JDBCConnection.getString(statement);
    } finally {
      statement.close();
    }

    return ret;
  }
  */

  /**
   * Returns the sample ids associated with an experiment.
   *
   * @param context the context
   * @param connection the connection
   * @param experimentId the experiment id
   * @return the sample ids
   * @throws SQLException the SQL exception
   */
  protected static List<Integer> getSampleIds(ServletContext context,
      Connection connection,
      int experimentId) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(EXPERIMENT_SAMPLES_SQL);

    List<Integer> ids = new ArrayList<Integer>();

    try {
      statement.setInt(1, experimentId);

      ids.addAll(JDBCConnection.getIntList(statement));

    } finally {
      statement.close();
    }

    return ids;
  }

  /**
   * Gets the sample.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the sample
   * @throws SQLException the SQL exception
   */
  protected static ResultsSetTable getSample(Connection connection,
      int sampleId) throws SQLException {

    PreparedStatement statement = connection
        .prepareStatement(Database.SAMPLE_SQL);

    ResultsSetTable table = null;

    try {
      statement.setInt(1, sampleId);

      table = JDBCConnection.resultSetTable(statement);
    } finally {
      statement.close();
    }

    return table;
  }

  /**
   * Process samples.
   *
   * @param connection the connection
   * @param userId the user id
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processSamples(Connection connection,
      int userId,
      ResultsSetTable table,
      JsonBuilder jsonArray) throws SQLException {
    processSamples(connection,
        userId,
        ALL_TAGS,
        ALL_ORGANISMS,
        table,
        jsonArray);
  }

  /**
   * Process samples.
   *
   * @param connection the connection
   * @param userId the user id
   * @param types the types
   * @param organisms the organisms
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processSamples(Connection connection,
      int userId,
      Set<Integer> types,
      Set<Integer> organisms,
      ResultsSetTable table,
      JsonBuilder jsonArray) throws SQLException {

    boolean isAdmin = WebAuthentication.getIsAdminOrSuper(connection, userId);

    Collection<Integer> userGroupIds = Groups.userGroups(connection, userId);

    while (table.next()) {
      int sampleId = table.getInt(0);

      int type = table.getInt(2);
      int organism = table.getInt(4);

      Collection<Integer> sampleGroupIds = Groups.sampleGroups(connection,
          sampleId);

      // Determine if this one of the user groups ids is in the sample group ids
      boolean inGroup = CollectionUtils.contains(userGroupIds, sampleGroupIds);

      if (!isAdmin && !inGroup) {
        continue;
      }

      // Skip if we are not the correct type
      if (types.size() != 0 && !types.contains(type)) {
        continue;
      }

      // Skip if we are not the correct organism
      if (organisms.size() != 0 && !organisms.contains(organism)) {
        continue;
      }

      int experimentId = table.getInt(1);

      // If you're not an admin, restrict which samples you can see.
      /*
       * boolean isLocked = !WebAuthentication.canViewSample(connection,
       * experimentId, sampleId, userId, isAdmin);
       */

      // boolean isLocked = Groups.isLocked(connection, sampleId);

      jsonArray.startObject();

      // if (views.contains("all") || views.contains("samples")) {
      jsonArray.add(EDB.HEADING_ID, sampleId);
      jsonArray.add(EDB.HEADING_EXPERIMENT, experimentId);
      jsonArray.add(EDB.HEADING_TYPE, type);
      jsonArray.add(EDB.HEADING_NAME_SHORT, table.getString(3));
      // sampleJSON.add(Application.HEADING_DESCRIPTION,
      // table.getDataAsString(i, 4));
      jsonArray.add(EDB.HEADING_ORGANISM, organism);

      // jsonArray.add(Application.HEADING_STATE,
      // SampleState.shortCode(SampleState.parse(isLocked)));

      jsonArray.add(EDB.HEADING_DATE, table.getString(5));

      // Add the groups
      groupsJson(connection, sampleGroupIds, jsonArray);

      /*
       * if (tagView) { jsonArray.startArray(Application.HEADING_TAGS);
       * getTagsJson(connection, sampleId, tags, jsonArray);
       * jsonArray.endArray(); }
       */

      /// if (personView) {
      // getPersonsJson(connection, sampleId, sampleJSON);
      // }

      /*
       * if (geoView) { getGeo(connection, sampleId, jsonArray); }
       */

      /*
       * if (filesView) { DatabaseResultsTable filesTable =
       * Vfs.getSampleFilesTable(connection, sampleId);
       * 
       * JsonArray filesJSON = new JsonArray();
       * 
       * processFiles(connection, filesTable, filesJSON);
       * 
       * sampleJSON.add(Application.HEADING_FILES, filesJSON); }
       */

      jsonArray.endObject();
    }
  }

  /**
   * Add the groups to the sample.
   *
   * @param connection the connection
   * @param groupIds the group ids
   * @param json the json
   * @throws SQLException the SQL exception
   */
  private static void groupsJson(Connection connection,
      final Collection<Integer> groupIds,
      JsonBuilder json) throws SQLException {

    json.startArray(EDB.HEADING_GROUPS);

    for (int id : groupIds) {
      json.add(id);
    }

    json.endArray();

  }

  /*
  private static void getPersonsJson(Connection connection,
      int sampleId,
      JsonBuilder sampleJSON) throws SQLException {

    String json = getSamplePersonsJson(connection, sampleId);

    if (json != null) {
      // cache.put(new Element(sampleId, json));

      sampleJSON.insert("persons", json);
    }
    // else {
    // sampleJSON.add("geo", constructPersonsJson(connection, sampleId));
    // }
  }
  */

  /*
  protected static String getSamplePersonsJson(Connection connection,
      int sampleId) throws SQLException {
    PreparedStatement statement = connection
        .prepareStatement(JSON_SAMPLE_PERSONS_SQL);

    String ret = null;

    try {
      statement.setInt(1, sampleId);

      // System.err.println("statement " + ret);

      ret = JDBCConnection.getString(statement);
    } finally {
      statement.close();
    }

    return ret;
  }
  */

  /**
   * Construct persons json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the string
   * @throws SQLException the SQL exception
   */
  public static String constructPersonsJson(Connection connection, int sampleId)
      throws SQLException {
    JsonBuilder jsonArray = JsonBuilder.create().startArray();

    constructPersonsJson(connection, sampleId, jsonArray);

    jsonArray.endArray();

    return jsonArray.toString();
  }

  /**
   * Construct persons json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  public static void constructPersonsJson(Connection connection,
      int sampleId,
      JsonBuilder jsonArray) throws SQLException {
    List<Integer> ids = getPersonIds(connection, sampleId);

    // JsonArray personsJSON = new JsonArray();

    for (int id : ids) {
      // JsonObject personJSON = new JsonObject();

      // personJSON.add(Application.HEADING_ID,
      // personTable.getDataAsInt(j, 1));
      // fileJSON.add(Application.HEADING_NAME, filestable.getDataAsString(j,
      // 1));
      // fileJSON.add(Application.HEADING_TYPE_ID, new
      // JsonString(filestable.getDataAsString(j, 2)));

      jsonArray.add(id);
    }

    // return personsJSON;
  }

  /*
  protected static void processSamplesPersons(Connection connection,
      int userId,
      Collection<Integer> sampleIds,
      JsonBuilder jsonArray) throws SQLException {

    for (int sampleId : sampleIds) {
      // JsonObject sampleJSON = JsonObject.create();
      jsonArray.startObject();

      // if (views.contains("all") || views.contains("samples")) {
      jsonArray.add(EDB.HEADING_ID, sampleId);

      getPersonsJson(connection, sampleId, jsonArray);

      // For testing only
      // sampleJSON.add("admin", isAdmin);
      // sampleJSON.add("can_view", WebAuthentication.canViewSample(connection,
      // context,
      // sampleId,
      // userId));
      // sampleJSON.add("s", sampleId);
      // sampleJSON.add("can_view", userId);

      jsonArray.endObject(); //// jsonArray.add(sampleJSON);
    }
  }
  */

  /**
   * Process sample.
   *
   * @param connection the connection
   * @param context the context
   * @param userId the user id
   * @param tags the tags
   * @param views the views
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   * @throws ParseException the parse exception
   */
  protected static void processSample(Connection connection,
      ServletContext context,
      int userId,
      Set<Integer> tags,
      Set<String> views,
      DatabaseResultsTable table,
      JsonBuilder jsonArray) throws SQLException, ParseException {

    // boolean isAdmin =
    // WebAuthentication.getIsAdminOrSuper(connection, userId);

    // output test values
    // JsonObject testJSON = new JsonObject();
    // testJSON.add("admin", isAdmin);
    // testJSON.add("size", table.getRowCount());
    // jsonArray.add(testJSON);

    views = TextUtils.toLowerCase(views);

    boolean tagView = views.contains("all") || views.contains("tags");

    boolean personView = views.contains("all") || views.contains("persons");

    boolean geoView = views.contains("all") || views.contains("geo");

    boolean filesView = views.contains("all") || views.contains("files");

    for (int i = 0; i < table.getRowCount(); ++i) {
      int sampleId = table.getInt(i, 0);
      int experimentId = table.getInt(i, 1);

      // If you're not an admin, restrict which samples you can see.
      // boolean isLocked = !WebAuthentication.canViewSample(connection,
      // context,
      // experimentId,
      // sampleId,
      // userId,
      // isAdmin);

      jsonArray.startObject();

      // JsonObject sampleJSON = new JsonObject();

      // if (views.contains("all") || views.contains("samples")) {
      jsonArray.add(EDB.HEADING_ID, sampleId);
      jsonArray.add(EDB.HEADING_EXPERIMENT, experimentId);
      jsonArray.add(EDB.HEADING_TYPE, table.getInt(i, 2));
      jsonArray.add(EDB.HEADING_NAME_SHORT, table.getString(i, 3));
      // sampleJSON.add(Application.HEADING_DESCRIPTION,
      // table.getDataAsString(i, 4));
      jsonArray.add(EDB.HEADING_SPECIES, table.getInt(i, 4));
      jsonArray.add(EDB.HEADING_DATE, table.getString(i, 5));

      // sampleJSON.add(Application.HEADING_STATE,
      // SampleState.shortCode(SampleState.parse(isLocked)));

      // }

      if (tagView) {
        jsonArray.add(EDB.HEADING_TAGS,
            getTagsJson(connection, sampleId, tags));
      }

      if (personView) {
        ResultsSetTable personTable = getPersonTable(connection, sampleId);

        // JsonArray personsJSON = new JsonArray();

        jsonArray.startArray("persons");

        while (personTable.next()) {
          jsonArray.add(personTable.getInt(1));
        }

        // sampleJson.add("persons", personsJSON);
        jsonArray.endArray();
      }

      if (geoView) {
        getGeo(connection, sampleId, jsonArray);
      }

      jsonArray.endObject();
    }
  }

  /**
   * Add all the tags for a sample to the json array.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @return the tags json
   * @throws SQLException the SQL exception
   */
  public static String getTagsJson(Connection connection, int sampleId)
      throws SQLException {
    return getTagsJson(connection, sampleId, ALL_TAGS);
  }

  /**
   * Gets the tags json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param jsonArray the json array
   * @return the tags json
   * @throws SQLException the SQL exception
   */
  public static void getTagsJson(Connection connection,
      int sampleId,
      JsonBuilder jsonArray) throws SQLException {
    // getTagsJson(connection, sampleId, jsonArray);

    constructTagsJson(connection, sampleId, jsonArray);
  }

  /**
   * Gets the tags json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @return the tags json
   * @throws SQLException the SQL exception
   */
  protected static String getTagsJson(Connection connection,
      int sampleId,
      final Set<Integer> tags) throws SQLException {
    JsonBuilder jsonArray = JsonBuilder.create().startArray();

    getTagsJson(connection, sampleId, tags, jsonArray);

    jsonArray.endArray();

    return jsonArray.toString();
  }

  /**
   * Returns the tags associated with a sample.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @param jsonArray the json array
   * @return the tags json
   * @throws SQLException the SQL exception
   */
  protected static void getTagsJson(Connection connection,
      int sampleId,
      final Set<Integer> tags,
      JsonBuilder jsonArray) throws SQLException {

    constructTagsJson(connection, sampleId, tags, jsonArray);
  }

  /**
   * Create the JSON for a specific sample tag.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tag the tag
   * @param jsonArray the json array
   * @return the tag json
   * @throws SQLException the SQL exception
   */
  protected static void getTagJson(Connection connection,
      int sampleId,
      int tag,
      JsonBuilder jsonArray) throws SQLException {

    constructTagJson(connection, sampleId, tag, jsonArray);
  }

  /**
   * Create the JSON for a specific sample.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param array the array
   * @throws SQLException the SQL exception
   */
  public static void constructTagsJson(Connection connection,
      int sampleId,
      JsonBuilder array) throws SQLException {

    ResultsSetTable table = getTextTagsTable(connection, sampleId);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getString(2));
      array.endObject();
    }

    table = getIntTagsTable(connection, sampleId);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getInt(2));
      array.endObject();
    }

    table = getFloatTagsTable(connection, sampleId);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getDataAsDouble(2));
      array.endObject();
    }
  }

  /**
   * Construct tag json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tag the tag
   * @param array the array
   * @throws SQLException the SQL exception
   */
  public static void constructTagJson(Connection connection,
      int sampleId,
      int tag,
      JsonBuilder array) throws SQLException {

    ResultsSetTable table = getTextTagTable(connection, sampleId, tag);

    boolean found = false;

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getString(2));
      array.endObject();

      found = true;
    }

    if (found) {
      return;
    }

    table = getIntTagTable(connection, sampleId, tag);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getInt(2));
      array.endObject();

      found = true;
    }

    if (found) {
      return;
    }

    table = getFloatTagTable(connection, sampleId, tag);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getDataAsDouble(2));
      array.endObject();
    }
  }

  /**
   * Construct tags json.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param tags the tags
   * @param array the array
   * @throws SQLException the SQL exception
   */
  public static void constructTagsJson(Connection connection,
      int sampleId,
      Collection<Integer> tags,
      JsonBuilder array) throws SQLException {

    ResultsSetTable table = getTextTagsTable(connection, sampleId, tags);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getString(2));
      array.endObject();
    }

    table = getIntTagsTable(connection, sampleId, tags);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getInt(2));
      array.endObject();
    }

    table = getFloatTagsTable(connection, sampleId, tags);

    while (table.next()) {
      array.startObject();
      array.add(EDB.HEADING_ID, table.getInt(1));
      array.add(EDB.HEADING_VALUE, table.getDataAsDouble(2));
      array.endObject();
    }
  }

  /*
  protected static String getSampleFieldsJson(Connection connection,
      int sampleId) throws SQLException {
    PreparedStatement statement = connection
        .prepareStatement(JSON_SAMPLE_FIELDS_SQL);

    String ret = null;

    try {
      statement.setInt(1, sampleId);

      // System.err.println("statement " + ret);

      ret = JDBCConnection.getString(statement);
    } finally {
      statement.close();
    }

    return ret;
  }
  */

  /*
   * protected static void processSamples(Connection connection, ResultsSetTable
   * table, JsonBuilder jsonArray) throws SQLException, ParseException { if
   * (table == null) { return; }
   * 
   * while (table.next()) { int sampleId = table.getInt(0);
   * 
   * //JsonObject sampleJSON = new JsonObject(); jsonArray.startObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, sampleId);
   * jsonArray.add(EDB.HEADING_EXPERIMENT, table.getInt(1));
   * jsonArray.add(EDB.HEADING_EXPRESSION_TYPE_ID, table.getInt(2));
   * jsonArray.add(EDB.HEADING_NAME, table.getString(3));
   * jsonArray.add(EDB.HEADING_DESCRIPTION, table.getString(4));
   * jsonArray.add(EDB.HEADING_SPECIES, table.getInt(5));
   * jsonArray.add(EDB.HEADING_DATE, table.getString(6));
   * 
   * //JsonArray tagsJSON = new JsonArray();
   * jsonArray.startObject(EDB.HEADING_TAGS);
   * 
   * ResultsSetTable fieldTable = getTextTagsTable(connection, sampleId);
   * 
   * while (fieldTable.next()) { //JsonObject fieldJSON = new JsonObject();
   * jsonArray.startObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, fieldTable.getInt(0));
   * jsonArray.add(EDB.HEADING_TAG_ID, fieldTable.getInt(1));
   * jsonArray.add(EDB.HEADING_VALUE, fieldTable.getString(2));
   * 
   * jsonArray.endObject(); }
   * 
   * fieldTable = getIntTagsTable(connection, sampleId);
   * 
   * while (table.next()) { //JsonObject fieldJSON = new JsonObject();
   * jsonArray.startObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, fieldTable.getInt(0));
   * jsonArray.add(EDB.HEADING_TAG_ID, fieldTable.getInt(1));
   * jsonArray.add(EDB.HEADING_VALUE, fieldTable.getInt(2));
   * 
   * jsonArray.endObject(); }
   * 
   * fieldTable = getFloatTagsTable(connection, sampleId);
   * 
   * while (table.next()) { //JsonObject fieldJSON = new JsonObject();
   * jsonArray.startObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, fieldTable.getInt(0));
   * jsonArray.add(EDB.HEADING_TAG_ID, fieldTable.getInt(1));
   * jsonArray.add(EDB.HEADING_VALUE, fieldTable.getDataAsDouble(2));
   * 
   * jsonArray.endObject(); }
   * 
   * jsonArray.endObject();
   * 
   * ResultsSetTable personTable = getPersonTable(connection, sampleId);
   * 
   * jsonArray.startArray("persons");
   * 
   * while (table.next()) { //JsonObject personJSON = new JsonObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, personTable.getInt(1));
   * //fileJSON.add(Application.HEADING_NAME, filestable.getDataAsString(j, 1));
   * //fileJSON.add(Application.HEADING_TYPE_ID, new
   * JsonString(filestable.getDataAsString(j, 2)));
   * 
   * //personsJSON.add(personJSON); }
   * 
   * jsonArray.endArray();
   * 
   * getGeo(connection, sampleId, jsonArray);
   * 
   * ResultsSetTable filesTable = Vfs.getSampleFilesTable(connection, sampleId);
   * 
   * jsonArray.startArray(EDB.HEADING_FILES);
   * 
   * while(filesTable.next()) { //JsonObject fileJSON = new JsonObject();
   * jsonArray.startObject();
   * 
   * jsonArray.add(EDB.HEADING_ID, filesTable.getInt(0));
   * jsonArray.add(EDB.HEADING_NAME, filesTable.getString(1));
   * jsonArray.add(EDB.HEADING_TYPE, filesTable.getInt(2));
   * 
   * jsonArray.endObject(); }
   * 
   * jsonArray.endArray(); } }
   */

  /**
   * Process experiments.
   *
   * @param connection the connection
   * @param personId the person id
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processExperiments(Connection connection,
      int personId,
      ResultsSetTable table,
      JsonBuilder jsonArray) throws SQLException {
    processExperiments(connection, personId, table, ALL_VIEW, jsonArray);
  }

  /**
   * Process experiments.
   *
   * @param connection the connection
   * @param personId the person id
   * @param table the table
   * @param views the views
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processExperiments(Connection connection,
      int personId,
      ResultsSetTable table,
      Set<String> views,
      JsonBuilder jsonArray) throws SQLException {

    while (table.next()) {
      int experimentId = table.getInt(0);

      // boolean isLocked = !WebAuthentication.canViewExperiment(connection,
      // experimentId,
      // personId);

      jsonArray.startObject(); // JsonObject json = new JsonObject();

      jsonArray.add(EDB.HEADING_ID, experimentId);
      jsonArray.add(EDB.HEADING_PUBLIC_ID, table.getString(1));
      jsonArray.add(EDB.HEADING_NAME_SHORT, table.getString(2));
      jsonArray.add(EDB.HEADING_DESCRIPTION, table.getString(3));
      jsonArray.add(EDB.HEADING_DATE, table.getString(4));
      // jsonArray.add(Application.HEADING_LOCKED, isLocked);

      jsonArray.endObject(); // jsonArray.add(json);
    }
  }

  /**
   * Process types.
   *
   * @param connection the connection
   * @param types the types
   * @param jsonArray the json array
   */
  protected static void processTypes(Connection connection,
      List<Type> types,
      JsonArray jsonArray) {

    for (Type type : types) {
      JsonObject fieldJSON = new JsonObject();

      // attributesJSON.add(Application.HEADING_ID, new
      // JSONInteger(table.getDataAsInt(i, 0)));
      fieldJSON.add(EDB.HEADING_ID, type.getId());
      fieldJSON.add(EDB.HEADING_NAME_SHORT, type.getName());

      jsonArray.add(fieldJSON);
    }
  }

  /**
   * Process types.
   *
   * @param table the table
   * @param jsonArray the json array
   */
  protected static void processTypes(DatabaseResultsTable table,
      JsonBuilder jsonArray) {

    for (int i = 0; i < table.getRowCount(); ++i) {
      // JsonObject fieldJSON = new JsonObject();
      jsonArray.startObject();

      // attributesJSON.add(Application.HEADING_ID, new
      // JSONInteger(table.getDataAsInt(i, 0)));
      jsonArray.add(EDB.HEADING_ID, table.getInt(i, 0));
      jsonArray.add(EDB.HEADING_NAME_SHORT, table.getString(i, 1));

      jsonArray.endObject();
    }
  }

  /**
   * Process sample files.
   *
   * @param connection the connection
   * @param sampleId the sample id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processSampleFiles(Connection connection,
      int sampleId,
      JsonBuilder jsonArray) throws SQLException {

    List<Integer> ids = Vfs.getSampleFiles(connection, sampleId);

    ResultsSetTable table = Vfs.getFilesTable(connection, ids);

    processFiles(connection, table, jsonArray);
  }

  /**
   * Process experiment files.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processExperimentFiles(Connection connection,
      int experimentId,
      JsonBuilder jsonArray) throws SQLException {

    ResultsSetTable table = Vfs.getExperimentFilesTable(connection,
        experimentId);

    processFiles(connection, table, jsonArray);
  }

  /**
   * Process experiment files dir.
   *
   * @param connection the connection
   * @param experimentId the experiment id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processExperimentFilesDir(Connection connection,
      int experimentId,
      JsonBuilder jsonArray) throws SQLException {

    ResultsSetTable table = Vfs.getExperimentFilesDirTable(connection,
        experimentId);

    processFiles(connection, table, jsonArray);
  }

  /**
   * Process files.
   *
   * @param connection the connection
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processFiles(Connection connection,
      ResultsSetTable table,
      JsonBuilder jsonArray) throws SQLException {
    while (table.next()) {
      int vfsId = table.getInt(0);

      // Json fileJson = new JsonObject();
      jsonArray.startObject();

      jsonArray.add(EDB.HEADING_ID, vfsId);
      jsonArray.add(EDB.HEADING_PID, table.getInt(1));
      jsonArray.add(EDB.HEADING_NAME_SHORT, table.getString(2));
      jsonArray.add(EDB.HEADING_TYPE, table.getInt(3));
      jsonArray.add(EDB.HEADING_DATE, table.getString(5));

      // JsonArray tagsJson = new JsonArray();
      // processVfsTags(connection, vfsId, tagsJson);
      // fileJson.add(Application.HEADING_TAGS, tagsJson);

      jsonArray.endObject();
    }
  }

  /**
   * Return the info for a particular file.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processFile(Connection connection,
      int vfsId,
      JsonBuilder jsonArray) throws SQLException {
    ResultsSetTable table = Vfs.getFileTable(connection, vfsId);

    processFiles(connection, table, jsonArray);
  }

  /**
   * Process files.
   *
   * @param connection the connection
   * @param context the context
   * @param personId the person id
   * @param table the table
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processFiles(Connection connection,
      ServletContext context,
      int personId,
      ResultsSetTable table,
      JsonArray jsonArray) throws SQLException {
    while (table.next()) {
      int vfsId = table.getInt(0);

      if (WebAuthentication
          .getCanViewFile(connection, context, personId, vfsId)) {
        table.next();

        JsonObject fileJSON = new JsonObject();

        fileJSON.add(EDB.HEADING_ID, vfsId);
        fileJSON.add(EDB.HEADING_PID, table.getInt(1));
        fileJSON.add(EDB.HEADING_NAME_SHORT, table.getString(2));
        fileJSON.add(EDB.HEADING_TYPE, table.getInt(3));

        jsonArray.add(fileJSON);
      }
    }
  }

  /**
   * Process vfs tags.
   *
   * @param connection the connection
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processVfsTags(Connection connection,
      JsonBuilder jsonArray) throws SQLException {

    List<Integer> ids = Vfs.getVfsTags(connection);

    DatabaseResultsTable table = getTagsTable(connection, ids);

    processTags(table, jsonArray);
  }

  /**
   * Process vfs tags.
   *
   * @param connection the connection
   * @param vfsId the vfs id
   * @param jsonArray the json array
   * @throws SQLException the SQL exception
   */
  protected static void processVfsTags(Connection connection,
      int vfsId,
      JsonBuilder jsonArray) throws SQLException {

    List<Integer> ids = Vfs.getVfsTags(connection, vfsId);

    DatabaseResultsTable table = getTagsTable(connection, ids);

    processTags(table, jsonArray);
  }

  /**
   * Gets the tags table.
   *
   * @param connection the connection
   * @param ids the ids
   * @return the tags table
   * @throws SQLException the SQL exception
   */
  protected static DatabaseResultsTable getTagsTable(Connection connection,
      List<Integer> ids) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(TAGS_SQL);

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

  /**
   * Process tags.
   *
   * @param table the table
   * @param jsonArray the json array
   */
  protected static void processTags(DatabaseResultsTable table,
      JsonBuilder jsonArray) {
    processTypes(table, jsonArray);
  }

  /**
   * Returns the attribute name for a given attribute id. This method takes care
   * of caching results to reduce database access.
   *
   * @param connection the connection
   * @param path the path
   * @return the tag id
   * @throws SQLException the SQL exception
   */
  /*
   * protected String getAttribute(ServletContext context, Connection
   * connection, int attributeId) throws SQLException {
   * 
   * ConcurrentIdTextStore map =
   * (ConcurrentIdTextStore)context.getAttribute(Application.
   * ATTRIBUTES_STORE_ATTRIBUTE);
   * 
   * if (map.contains(attributeId)) { return map.get(attributeId); }
   * 
   * PreparedStatement statement = connection.prepareStatement(ATTRIBUTE_SQL);
   * 
   * String ret = null;
   * 
   * try { statement.setInt(1, attributeId);
   * 
   * DatabaseResultsTable table = JDBCConnection.getTable(statement);
   * 
   * ret = table.getRowCount() == 1 ? table.getDataAsString(0, 0) : null; }
   * finally { statement.close(); }
   * 
   * if (ret != null) { map.put(attributeId, ret); }
   * 
   * return ret; }
   */

  protected static int getTagId(Connection connection, Path path)
      throws SQLException {
    return getTagId(connection, path.toString());
  }

  /**
   * Returns the search field id for a keyword, or -1 if the keyword does not
   * exist in the database.
   *
   * @param connection the connection
   * @param name the name
   * @return the tag id
   * @throws SQLException the SQL exception
   */
  protected static int getTagId(Connection connection, String name)
      throws SQLException {

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

  public static int getTagId(JdbcTemplate jdbcTemplate, String name)
      throws SQLException {
    return Database.getId(jdbcTemplate, TAG_ID_SQL, name);
  }

  /**
   * Gets the sample id from geo accession.
   *
   * @param connection the connection
   * @param accession the accession
   * @return the sample id from geo accession
   * @throws SQLException the SQL exception
   */
  protected static int getSampleIdFromGeoAccession(Connection connection,
      String accession) throws SQLException {

    PreparedStatement statement = connection.prepareStatement(GEO_SAMPLE_SQL);

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
   * @return the connection
   * @throws SQLException the SQL exception
   */
  public Connection getConnection() throws SQLException {
    if (mDs == null) {
      return null;
    }

    return mDs.getConnection();
  }

  /**
   * Parses a string as a number returning -1 if the string is a badly formed
   * number.
   *
   * @param id the id
   * @return the int
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

  /**
   * Authenticate key.
   *
   * @param context the context
   * @param request the request
   * @param connection the connection
   * @param key the key
   * @return the int
   */
  protected int authenticateKey(ServletContext context,
      HttpServletRequest request,
      Connection connection,
      String key) {
    return 6;
  }

  /**
   * Process samples.
   *
   * @param connection the connection
   * @param context the context
   * @param person the person
   * @param table the table
   * @param samples the samples
   * @param organisms the organisms
   * @param experiments the experiments
   * @param expressionTypes the expression types
   * @throws SQLException the SQL exception
   */
  public static void processSamples(Connection connection,
      ServletContext context,
      Person person,
      ResultsSetTable table,
      Collection<Sample> samples,
      Map<Integer, Species> organisms,
      Map<Integer, Experiment> experiments,
      TypeMap expressionTypes) throws SQLException {
    processSamples(connection,
        context,
        person.getId(),
        table,
        samples,
        organisms,
        experiments,
        expressionTypes);
  }

  /**
   * Process samples.
   *
   * @param connection the connection
   * @param context the context
   * @param userId the user id
   * @param table the table
   * @param samples the samples
   * @param organisms the organisms
   * @param experiments the experiments
   * @param expressionTypes the expression types
   * @throws SQLException the SQL exception
   */
  public static void processSamples(Connection connection,
      ServletContext context,
      int userId,
      ResultsSetTable table,
      Collection<Sample> samples,
      Map<Integer, Species> organisms,
      Map<Integer, Experiment> experiments,
      TypeMap expressionTypes) throws SQLException {

    // Map<Integer, Organism> organisms = Database.getOrganisms(connection);
    // Map<Integer, Experiment> experiments =
    // Database.getExperiments(connection);
    // Map<Integer, Type> expressionTypes =
    // Database.getExpressionTypes(connection);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    while (table.next()) {
      int sampleId = table.getInt(0);

      if (!WebAuthentication.canViewSample(connection, sampleId, userId)) {
        continue;
      }

      Date date = null;

      try {
        date = formatter.parse(table.getString(5));
      } catch (ParseException e) {
        e.printStackTrace();
      }

      Sample sample = new Sample(sampleId, experiments.get(table.getInt(1)),
          expressionTypes.get(table.getInt(2)), table.getString(3),
          organisms.get(table.getInt(4)), date);

      samples.add(sample);
    }
  }

  /*
   * protected static List<FileRecord> getFiles(Connection connection, int
   * personId, List<Integer> ids, File dataDirectory) throws SQLException,
   * IOException, SQLException {
   * 
   * List<FileRecord> files = new ArrayList<FileRecord>();
   * 
   * for (Integer id : ids) { FileRecord file = getFile(connection, personId,
   * id, dataDirectory);
   * 
   * if (file == null) { continue; }
   * 
   * files.add(file); }
   * 
   * return files; }
   * 
   * protected static FileRecord getFile(Connection connection, int personId,
   * int fileId, File dataDirectory) throws SQLException {
   * 
   * FileRecord fileRecord = null;
   * 
   * PreparedStatement statement = connection.prepareStatement(FILE_SQL);
   * 
   * try { statement.setInt(1, fileId);
   * 
   * DatabaseResultsTable table = JDBCConnection.getTable(statement);
   * 
   * if (table.getRowCount() == 0) { return null; }
   * 
   * int sampleId = table.getDataAsInt(0, 1);
   * 
   * if (!Authentication.getCanViewSample(connection, sampleId, personId)) {
   * return null; }
   * 
   * 
   * 
   * String name = table.getDataAsString(0, 2);
   * 
   * String path = table.getDataAsString(0, 3);
   * 
   * fileRecord = new FileRecord(fileId, name, path); } finally {
   * statement.close(); }
   * 
   * return fileRecord; }
   */
}
