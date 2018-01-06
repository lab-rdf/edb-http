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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.jebtk.core.cryptography.Cryptography;
import org.jebtk.core.cryptography.CryptographyException;
import org.jebtk.core.cryptography.TOTP;
import org.jebtk.core.json.Json;
import org.jebtk.core.json.JsonBuilder;
import org.jebtk.core.text.TextUtils;
import org.jebtk.database.DatabaseResultsTable;
import org.jebtk.database.JDBCConnection;

import edu.columbia.rdf.edb.Person;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

// TODO: Auto-generated Javadoc
/**
 * The Class WebAuthentication.
 */
public class WebAuthentication {
  /**
   * Change codes every 5 minutes.
   */
  public static final long OTK_STEP_SIZE = 300000;

  /** The Constant SQL_DELETE_SESSION. */
  private static final String SQL_DELETE_SESSION = "DELETE FROM login_sessions WHERE login_sessions.person_id = ?";

  /** The Constant SQL_ADD_SESSION. */
  private static final String SQL_ADD_SESSION = "INSERT INTO login_sessions (key, person_id) VALUES (?, ?)";

  /** The Constant LOGIN_IP_SQL. */
  public static final String LOGIN_IP_SQL = "SELECT login_ip_address.ip_address, login_ip_address.person_id FROM login_ip_address";

  /** The Constant LOGIN_SQL. */
  private static final String LOGIN_SQL = "SELECT login_persons.person_id, login_persons.password_hash_salted, login_persons.salt FROM login_persons WHERE login_persons.user_name = ?";

  // private final static String SAMPLE_PERMISSIONS_SQL =
  // "SELECT sample_permissions.id FROM sample_permissions WHERE
  // sample_permissions.sample_id = ? AND sample_permissions.person_id = ?";

  // private final static String EXPERIMENT_PERMISSIONS_SQL =
  // "SELECT experiment_permissions.id FROM experiment_permissions WHERE
  // experiment_permissions.experiment_id = ? AND experiment_permissions.person_id
  // = ?";

  // private final static String VFS_PERMISSIONS_SQL =
  // "SELECT vfs_permissions.id FROM vfs_permissions WHERE vfs_permissions.vfs_id
  // = ? AND vfs_permissions.person_id = ?";

  /** The Constant PERSON_SQL. */
  private static final String PERSON_SQL = "SELECT persons.id, persons.first_name, persons.last_name, persons.email FROM persons WHERE persons.id = ?";

  /** The Constant USER_SQL. */
  private static final String USER_SQL = "SELECT persons.id FROM persons WHERE persons.public_uuid = ?";

  /** The Constant BLOCKED_IP_ADDRESS. */
  static final String BLOCKED_IP_ADDRESS = "blocked";

  /** The Constant VALIDATE_IP_SQL. */
  public static final String VALIDATE_IP_SQL = "SELECT login_ip_address.id FROM login_ip_address WHERE login_ip_address.person_id = ? AND (login_ip_address.ip_address = '*' OR login_ip_address.ip_address LIKE ?)";

  /** The Constant KEY_SQL. */
  public static final String KEY_SQL = "SELECT persons.api_key FROM persons WHERE persons.id = ?";

  /** The Constant SQL_LOGIN_ATTEMPT. */
  private static final String SQL_LOGIN_ATTEMPT = "INSERT INTO login_attempts (person_id, ip_address, success) VALUES (?, ?, ?)";

  /** The Constant USER_TYPE_SQL. */
  private static final String USER_TYPE_SQL = "SELECT persons.user_type_id FROM persons WHERE persons.id = ?";

  /** The Constant JSON_AUTH_FAILED_USER. */
  public static final Json JSON_AUTH_FAILED_USER = new JsonAuthFailed("user-id");

  /** The Constant JSON_AUTH_FAILED_OTK. */
  public static final Json JSON_AUTH_FAILED_OTK = new JsonAuthFailed("totp");

  /** The Constant JSON_AUTH_FAILED_SAMPLE. */
  public static final Json JSON_AUTH_FAILED_SAMPLE = new JsonAuthFailed("sample");

  /** The Constant JSON_AUTH_FAILED_FIELD. */
  public static final Json JSON_AUTH_FAILED_FIELD = new JsonAuthFailed("field");

  /** The Constant JSON_AUTH_FAILED_EXPERIMENT. */
  public static final Json JSON_AUTH_FAILED_EXPERIMENT = new JsonAuthFailed("experiment");

  /** The Constant JSON_AUTH_FAILED_PATH. */
  public static final Json JSON_AUTH_FAILED_PATH = new JsonAuthFailed("path");

  /**
   * Validate purely by ip address and negate users having to login.
   *
   * @param connection
   *          the connection
   * @param ipAddress
   *          the ip address
   * @return the int
   * @throws SQLException
   *           the SQL exception
   * @throws ParseException
   *           the parse exception
   */
  public static int validateIpLogin(Connection connection, String ipAddress) throws SQLException, ParseException {

    int userId = -1;

    if (ipAddress == null) {
      return userId;
    }

    PreparedStatement statement = connection.prepareStatement(LOGIN_IP_SQL);

    try {
      DatabaseResultsTable table = JDBCConnection.getTable(statement);

      for (int i = 0; i < table.getRowCount(); ++i) {
        String ip = table.getString(i, 0);

        if (ipAddress.startsWith(ip) || ip.equals("*")) {
          userId = table.getInt(i, 1);

          break;
        }
      }
    } finally {
      statement.close();
    }

    return userId;
  }

  /**
   * Standard validation of user password combination.
   *
   * @param connection
   *          the connection
   * @param user
   *          the user
   * @param password
   *          the password
   * @return the int
   * @throws SQLException
   *           the SQL exception
   * @throws CryptographyException
   *           the cryptography exception
   * @throws ParseException
   *           the parse exception
   */
  public static int validateLogin(Connection connection, String user, String password)
      throws SQLException, CryptographyException, ParseException {

    if (user == null || password == null) {
      return -1;
    }

    PreparedStatement statement;

    // first get the salt

    int userId = -1;
    String salt = null;
    String passwordHashSalted = null;

    statement = connection.prepareStatement(LOGIN_SQL);

    try {
      statement.setString(1, user);

      DatabaseResultsTable table = JDBCConnection.getTable(statement);

      if (table.getRowCount() == 1) {
        userId = table.getInt(0, 0);
        passwordHashSalted = table.getString(0, 1);
        salt = table.getString(0, 2);
      }
    } finally {
      statement.close();
    }

    if (userId == -1) {
      return -1;
    }

    String testPasswordHashSalted = Cryptography.getSHA512Hash(password, salt);

    if (!testPasswordHashSalted.equals(passwordHashSalted)) {
      return -1;
    }

    return userId;
  }

  /**
   * Gets the person.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the person
   * @throws SQLException
   *           the SQL exception
   * @throws ParseException
   *           the parse exception
   */
  public static Person getPerson(Connection connection, int userId) throws SQLException, ParseException {
    if (userId == -1) {
      return null;
    }

    Person person = null;

    PreparedStatement statement = connection.prepareStatement(PERSON_SQL);

    try {
      statement.setInt(1, userId);

      DatabaseResultsTable table = JDBCConnection.getTable(statement);

      if (table.getRowCount() == 1) {
        person = new Person(table.getInt(0, 0), table.getString(0, 1), table.getString(0, 2), table.getString(0, 3));
      }
    } finally {
      statement.close();
    }

    return person;
  }

  /**
   * Creates the login session.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the string
   * @throws SQLException
   *           the SQL exception
   */
  public static String createLoginSession(Connection connection, int userId) throws SQLException {
    if (userId == -1) {
      return null;
    }

    PreparedStatement statement = connection.prepareStatement(SQL_DELETE_SESSION);

    try {
      statement.setInt(1, userId);

      statement.execute();
    } finally {
      statement.close();
    }

    String key = Cryptography.generateRandAlphaNumId(64);

    statement = connection.prepareStatement(SQL_ADD_SESSION);

    try {
      statement.setString(1, key);
      statement.setInt(2, userId);

      statement.execute();

    } finally {
      statement.close();
    }

    return key;
  }

  /**
   * Can view sample.
   *
   * @param connection
   *          the connection
   * @param sampleId
   *          the sample id
   * @param person
   *          the person
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean canViewSample(Connection connection, int sampleId, Person person) throws SQLException {
    return canViewSample(connection, sampleId, person.getId());
  }

  /**
   * Determines whether a user can view a sample by either its experiment being
   * viewable, or else the sample.
   *
   * @param connection
   *          the connection
   * @param experimentId
   *          the experiment id
   * @param sampleId
   *          the sample id
   * @param userId
   *          the user id
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean canViewSample(Connection connection, int experimentId, int sampleId, int userId)
      throws SQLException {

    return canViewSample(connection, experimentId, sampleId, userId, getIsAdminOrSuper(connection, userId));
  }

  /**
   * Determines whether a user can view a sample based on either the experiment id
   * or the sample id.
   *
   * @param connection
   *          the connection
   * @param experimentId
   *          the experiment id
   * @param sampleId
   *          the sample id
   * @param userId
   *          the user id
   * @param isAdmin
   *          the is admin
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean canViewSample(Connection connection, int experimentId, int sampleId, int userId,
      boolean isAdmin) throws SQLException {

    // return isAdmin ||
    // canViewExperiment(connection, experimentId, userId) ||
    // canViewSample(connection, sampleId, userId);

    return isAdmin || canViewSample(connection, sampleId, userId);
  }

  /**
   * Returns true if the user can view one of the given sample ids.
   *
   * @param connection
   *          the connection
   * @param sampleIds
   *          the sample ids
   * @param userId
   *          the user id
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean canViewSample(Connection connection, final Collection<Integer> sampleIds, int userId)
      throws SQLException {
    for (int sampleId : sampleIds) {
      if (canViewSample(connection, sampleId, userId)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Can view sample.
   *
   * @param connection
   *          the connection
   * @param sampleId
   *          the sample id
   * @param userId
   *          the user id
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  @SuppressWarnings("unchecked")
  public static boolean canViewSample(Connection connection, int sampleId, int userId) throws SQLException {

    /*
     * return canViewObject(connection, sampleId, userId,
     * CacheManager.getInstance().getCache("sample-view-cache"),
     * SAMPLE_PERMISSIONS_SQL);
     */

    if (getIsAdminOrSuper(connection, userId)) {
      return true;
    }

    Cache cache = CacheManager.getInstance().getCache("sample-view-cache");

    //
    // See if the item has been cached as viewable or not.
    //

    Element ce = cache.get(userId);

    if (ce == null) {
      ce = new Element(userId, new HashMap<Integer, Boolean>());

      cache.put(ce);
    }

    Map<Integer, Boolean> viewSet = (Map<Integer, Boolean>) ce.getObjectValue();

    // If we have already tested that we can view the sample, return true
    if (viewSet.containsKey(sampleId)) {
      return viewSet.get(sampleId);
    }

    boolean ret = Groups.userInSampleGroups(connection, userId, sampleId);

    viewSet.put(sampleId, ret);

    return ret;
  }

  /*
   * public static boolean canViewExperiment(Connection connection, final
   * Collection<Integer> experimentIds, int userId) throws SQLException { for (int
   * experimentId : experimentIds) { if (canViewExperiment(connection,
   * experimentId, userId)) { return true; } }
   * 
   * return false; }
   */

  /**
   * Returns true if the person is allowed to view a particular experiment, false
   * otherwise.
   *
   * @param connection
   *          the connection
   * @param objectId
   *          the object id
   * @param userId
   *          the user id
   * @param cache
   *          the cache
   * @param sql
   *          the sql
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  /*
   * @SuppressWarnings("unchecked") public static synchronized boolean
   * canViewExperiment(Connection connection, int experimentId, int userId) throws
   * SQLException {
   * 
   * return canViewObject(connection, experimentId, userId,
   * CacheManager.getInstance().getCache("experiment-view-cache"),
   * EXPERIMENT_PERMISSIONS_SQL); }
   */

  /**
   * Object view permission is similar across different entities such as
   * experiments and samples so this generic method handles checking and caching
   * whether an object can be viewed.
   * 
   * @param connection
   * @param context
   * @param objectId
   * @param userId
   * @param cache
   * @param sql
   * @return
   * @throws SQLException
   */
  @SuppressWarnings("unchecked")
  public static boolean canViewObject(Connection connection, int objectId, int userId, Cache cache, String sql)
      throws SQLException {

    // if (!checkViewPermissionsEnabled(context)) {
    // return true;
    // }

    // At given privileges you can look at anything
    if (getIsAdminOrSuper(connection, userId)) {
      return true;
    } else {
      boolean ret = false;

      //
      // See if the item has been cached as viewable or not.
      //

      Element ce = cache.get(userId);

      if (ce == null) {
        ce = new Element(userId, new HashMap<Integer, Boolean>());

        cache.put(ce);
      }

      Map<Integer, Boolean> viewSet = (Map<Integer, Boolean>) ce.getObjectValue();

      // If we have already tested that we can view the sample, return true
      if (viewSet.containsKey(objectId)) {
        ret = viewSet.get(objectId);
      } else {

        //
        // Last resort, poll the database
        //

        PreparedStatement statement = connection.prepareStatement(sql);

        try {
          statement.setInt(1, objectId);
          statement.setInt(2, userId);

          // System.err.println("web auth " + statement);

          ret = JDBCConnection.hasRecords(statement);
        } finally {
          statement.close();
        }

        viewSet.put(objectId, ret);
      }

      return ret;
    }

  }

  /**
   * Gets the can view file.
   *
   * @param connection
   *          the connection
   * @param context
   *          the context
   * @param vfsId
   *          the vfs id
   * @param person
   *          the person
   * @return the can view file
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean getCanViewFile(Connection connection, ServletContext context, int vfsId, Person person)
      throws SQLException {
    return getCanViewFile(connection, context, vfsId, person.getId());
  }

  /**
   * Returns true if user can get access to sample data based on being able to
   * view a sample or experiment this file belongs to.
   *
   * @param connection
   *          the connection
   * @param context
   *          the context
   * @param vfsId
   *          the vfs id
   * @param userId
   *          the user id
   * @return the can view file
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean getCanViewFile(Connection connection, ServletContext context, int vfsId, int userId)
      throws SQLException {

    /*
     * return canViewObject(connection, context, vfsId, userId,
     * CacheManager.getInstance().getCache("file-view-cache"), VFS_PERMISSIONS_SQL);
     */

    // At given privileges you can look at anything
    if (getIsAdminOrSuper(connection, userId)) {
      return true;
    }

    //
    // See if the item has been cached as viewable or not.
    //

    Cache cache = CacheManager.getInstance().getCache("file-view-cache");

    Element ce = cache.get(userId);

    if (ce == null) {
      ce = new Element(userId, new HashMap<Integer, Boolean>());

      cache.put(ce);
    }

    @SuppressWarnings("unchecked")
    Map<Integer, Boolean> viewSet = (Map<Integer, Boolean>) ce.getObjectValue();

    // If we have already tested that we can view the sample, return true
    if (viewSet.containsKey(vfsId)) {
      return viewSet.get(vfsId);
    }

    //
    // First see if we can find samples associated with the
    // vsf id

    List<Integer> ids = Vfs.getSamples(connection, vfsId);

    if (canViewSample(connection, ids, userId)) {
      viewSet.put(vfsId, true);

      return true;
    }

    // See if it is an experiment

    // ids = Vfs.getExperiments(connection, vfsId);

    // if (canViewExperiment(connection, ids, userId)) {
    // viewSet.put(vfsId, true);
    //
    // return true;
    // }

    // The file is not viewable as an experiment or sample so conclude
    // user cannot look at it

    viewSet.put(vfsId, false);

    return false;
  }

  /**
   * Returns if the user is a global user or admin since this means they can view
   * any samples.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the checks if is admin or super
   * @throws SQLException
   *           the SQL exception
   * @throws IllegalArgumentException
   *           the illegal argument exception
   * @throws IllegalStateException
   *           the illegal state exception
   * @throws CacheException
   *           the cache exception
   */
  public static boolean getIsAdminOrSuper(Connection connection, int userId) throws SQLException {
    UserType type = getUserType(connection, userId);

    return type == UserType.ADMINISTRATOR || type == UserType.SUPERUSER;
  }

  /**
   * Gets the checks if is administrator.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the checks if is administrator
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean getIsAdministrator(Connection connection, int userId) throws SQLException {
    return getUserType(connection, userId) == UserType.ADMINISTRATOR;
  }

  /**
   * Returns true if the user is an admin.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the checks if is superuser
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean getIsSuperuser(Connection connection, int userId) throws SQLException {
    return getUserType(connection, userId) == UserType.SUPERUSER;
  }

  /**
   * Returns the user type using a cache to speed up the check.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the user type
   * @throws SQLException
   *           the SQL exception
   */
  public static UserType getUserType(Connection connection, int userId) throws SQLException {
    Cache cache = CacheManager.getInstance().getCache("user-type-cache");

    Element ce = cache.get(userId);

    if (ce != null) {
      return (UserType) ce.getObjectValue();
    }

    UserType type = null;

    //
    // Last resort, poll the database
    //

    PreparedStatement statement = connection.prepareStatement(USER_TYPE_SQL);

    int typeId = -1;

    try {
      statement.setInt(1, userId);

      typeId = JDBCConnection.getInt(statement);
    } finally {
      statement.close();
    }

    type = UserType.getFromId(typeId);

    cache.put(new Element(userId, type));

    return type;
  }

  /**
   * Authenicate a user based on an user id, their ip address and a one time
   * random key.
   *
   * @param context
   *          the context
   * @param request
   *          the request
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @param totp
   *          the totp
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean totpAuthUser(ServletContext context, HttpServletRequest request, Connection connection,
      int userId, int totp) throws SQLException {

    return totpAuthUser(context, request, connection, userId, totp, (long) context.getAttribute("totp-step"));
  }

  /**
   * Totp auth user.
   *
   * @param context
   *          the context
   * @param request
   *          the request
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @param totp
   *          the totp
   * @param step
   *          the step
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean totpAuthUser(ServletContext context, HttpServletRequest request, Connection connection,
      int userId, int totp, long step) throws SQLException {

    if (checkAuthEnabled(context)) {
      return strictTOTPAuthUser(context, request, connection, userId, totp, step);
    } else {
      return true;
    }
  }

  /**
   * Strict TOTP auth user. Always authenticate.
   *
   * @param context
   *          the context
   * @param request
   *          the request
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @param totp
   *          the totp
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean strictTOTPAuthUser(ServletContext context, HttpServletRequest request, Connection connection,
      int userId, int totp) throws SQLException {
    return strictTOTPAuthUser(context, request, connection, userId, totp, (long) context.getAttribute("totp-step"));
  }

  /**
   * Strict TOTP auth user.
   *
   * @param context
   *          the context
   * @param request
   *          the request
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @param totp
   *          the totp
   * @param step
   *          the step
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean strictTOTPAuthUser(ServletContext context, HttpServletRequest request, Connection connection,
      int userId, int totp, long step) throws SQLException {

    if (userId == -1) {
      return false;
    }

    // First validate the ip address
    boolean validIp = validateIPAddress(connection, context, userId, request.getRemoteAddr());

    if (!validIp) {
      return false;
    }

    // Now check the one time key is valid

    String key = getKey(context, connection, userId);

    if (TextUtils.isNullOrEmpty(key)) {
      return false;
    }

    // return TOTP.totpAuth(key, totp, step);

    return totpAuth(userId, key, totp, step);
  }

  /**
   * Totp auth.
   *
   * @param userId
   *          the user id
   * @param key
   *          the key
   * @param totp
   *          the totp
   * @param step
   *          the step
   * @return true, if successful
   */
  private static boolean totpAuth(int userId, String key, int totp, long step) {
    return totpAuth(userId, key, totp, step, System.currentTimeMillis(), 0);
  }

  /**
   * Totp auth.
   *
   * @param userId
   *          the user id
   * @param key
   *          the key
   * @param totp
   *          the totp
   * @param step
   *          the step
   * @param time
   *          the time
   * @param epoch
   *          the epoch
   * @return true, if successful
   */
  private static boolean totpAuth(int userId, String key, int totp, long step, long time, long epoch) {

    long counter = TOTP.getCounter(time, epoch, step);

    Cache tcCache = CacheManager.getInstance().getCache("totp-tc-cache");
    // Cache otkCache = CacheManager.getInstance().getCache("totp-totp-cache");

    Element ce = tcCache.get(userId);

    long cachedCounter = -1;

    if (ce != null) {
      cachedCounter = (Long) ce.getObjectValue();

      // System.err.println("cache " + cachedCounter + " " + counter);
    }

    // ce = otkCache.get(userId);

    // long cachedOtk = -1;

    // if (ce != null) {
    // cachedOtk = (Long)ce.getObjectValue();
    // }

    // The client and server are in the same counter frame so we don't
    // have to validate further
    if (counter == cachedCounter) {
      return true;
    }

    boolean auth = TOTP.totpAuth(key, totp, time, epoch, step);

    if (auth) {
      tcCache.put(new Element(userId, counter));
      // otkCache.put(new Element(userId, totp));
    }

    return auth;
  }

  /**
   * Returns true if authentication has been enabled.
   *
   * @param context
   *          the context
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean checkAuthEnabled(ServletContext context) {
    return context.getAttribute("auth-enabled") != null;
  }

  /**
   * Returns true if view permission checking is enabled.
   *
   * @param context
   *          the context
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean checkViewPermissionsEnabled(ServletContext context) {
    return context.getAttribute("view-permissions-enabled") != null;
  }

  /**
   * Logs that the user made an attempt to login and how successful it was.
   *
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @param ipAddress
   *          the ip address
   * @param success
   *          the success
   * @throws SQLException
   *           the SQL exception
   */
  public static void logAttempt(Connection connection, int userId, String ipAddress, boolean success)
      throws SQLException {

    PreparedStatement statement = connection.prepareStatement(SQL_LOGIN_ATTEMPT);

    try {
      statement.setInt(1, userId);
      statement.setString(2, ipAddress);
      statement.setBoolean(3, success);

      statement.execute();
    } finally {
      statement.close();
    }
  }

  /**
   * Get a user's key for validating login.
   *
   * @param context
   *          the context
   * @param connection
   *          the connection
   * @param userId
   *          the user id
   * @return the key
   * @throws SQLException
   *           the SQL exception
   */
  public static String getKey(ServletContext context, Connection connection, int userId) throws SQLException {
    Cache cache = CacheManager.getInstance().getCache("key-cache");

    Element ce = cache.get(userId);

    if (ce != null) {
      return (String) ce.getObjectValue();
    }

    PreparedStatement statement = connection.prepareStatement(KEY_SQL);

    String key = null;

    try {
      statement.setInt(1, userId);

      key = JDBCConnection.getString(statement);
    } finally {
      statement.close();
    }

    cache.put(new Element(userId, key));

    return key;
  }

  /**
   * Validate that the reported user is accessing from a valid ip address.
   *
   * @param connection
   *          the connection
   * @param context
   *          the context
   * @param person
   *          the person
   * @param ipAddress
   *          the ip address
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean validateIPAddress(Connection connection, ServletContext context, int person, String ipAddress)
      throws SQLException {
    Cache cache = CacheManager.getInstance().getCache("ip_add_cache");

    Element ce = cache.get(person);

    if (ce != null) {
      // If the cache contains blocked, then this user is not allowed
      // to login in from this ip address

      String cachedIpAddress = (String) ce.getObjectValue();

      if (cachedIpAddress.equals(BLOCKED_IP_ADDRESS)) {
        return false;
      }

      if (cachedIpAddress.equals(ipAddress)) {
        return true;
      }
    }

    // We had a cache miss or else the ip address does not match the
    // cache so we resort to a longer check

    boolean valid = false;

    PreparedStatement statement = connection.prepareStatement(VALIDATE_IP_SQL);

    try {
      statement.setInt(1, person);
      statement.setString(2, ipAddress);

      valid = JDBCConnection.hasRecords(statement);
    } finally {
      statement.close();
    }

    if (valid) {
      cache.put(new Element(person, ipAddress));
    } else {
      cache.put(new Element(person, BLOCKED_IP_ADDRESS));
    }

    return valid;
  }

  /**
   * Validate topt.
   *
   * @param context
   *          the context
   * @param request
   *          the request
   * @param connection
   *          the connection
   * @param person
   *          the person
   * @param totp
   *          the totp
   * @return true, if successful
   * @throws SQLException
   *           the SQL exception
   */
  public static boolean validateTopt(ServletContext context, HttpServletRequest request, Connection connection,
      int person, int totp) throws SQLException {
    String key = getKey(context, connection, person);

    if (key == null) {
      return false;
    }

    Cache cache = CacheManager.getInstance().getCache("auth_cache");

    Element ce = cache.get(person);

    if (ce != null) {
      if (ce.getObjectValue().equals(totp)) {
        return true;
      }
    }

    // Cache miss so do a full check

    long step = (Long) context.getAttribute("topt-step");

    boolean ret = TOTP.totp256Auth(key, totp, step);

    if (ret) {
      // Only store the for 5 mins if it is valid
      cache.put(new Element(person, totp));
    }

    return ret;
  }

  /**
   * Returns the person id given their public id (which is used for authentication
   * purposes via the api).
   *
   * @param context
   *          the context
   * @param connection
   *          the connection
   * @param publicId
   *          the public id
   * @return the user id
   * @throws SQLException
   *           the SQL exception
   */
  public static synchronized int getUserId(ServletContext context, Connection connection, String publicId)
      throws SQLException {
    Cache cache = CacheManager.getInstance().getCache("user-id-cache");

    Element ce = cache.get(publicId);

    if (ce != null) {
      return (int) ce.getObjectValue();
    }

    PreparedStatement statement = connection.prepareStatement(USER_SQL);

    int userId = -1;

    try {
      statement.setString(1, publicId);

      userId = JDBCConnection.getInt(statement);
    } finally {
      statement.close();
    }

    cache.put(new Element(publicId, userId));

    return userId;
  }

  /**
   * Auth error.
   *
   * @param reason
   *          the reason
   * @param json
   *          the json
   */
  public static void authError(String reason, JsonBuilder json) {
    error("auth-fail", reason, json);
  }

  /**
   * Error.
   *
   * @param error
   *          the error
   * @param reason
   *          the reason
   * @param json
   *          the json
   */
  public static void error(String error, String reason, JsonBuilder json) {
    json.startObject();
    json.add(error, reason);
    json.endObject();
  }

  /**
   * totp auth error.
   *
   * @param json
   *          the json
   */
  public static void otkAuthError(JsonBuilder json) {
    authError("totp", json);
  }

  /**
   * Sample auth error.
   *
   * @param json
   *          the json
   */
  public static void sampleAuthError(JsonBuilder json) {
    authError("sample", json);
  }

  /**
   * User auth error.
   *
   * @param json
   *          the json
   */
  public static void userAuthError(JsonBuilder json) {
    authError("user", json);
  }

  /**
   * Exp auth error.
   *
   * @param json
   *          the json
   */
  public static void expAuthError(JsonBuilder json) {
    authError("experiment", json);
  }

  /**
   * Path auth error.
   *
   * @param json
   *          the json
   */
  public static void pathAuthError(JsonBuilder json) {
    authError("path", json);
  }

  /**
   * Field auth error.
   *
   * @param json
   *          the json
   */
  public static void fieldAuthError(JsonBuilder json) {
    authError("field", json);
  }

}
