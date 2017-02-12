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

import org.abh.common.cryptography.Cryptography;
import org.abh.common.cryptography.CryptographyException;
import org.abh.common.cryptography.TOTP;
import org.abh.common.database.DatabaseResultsTable;
import org.abh.common.database.JDBCConnection;
import org.abh.common.json.Json;
import org.abh.common.json.JsonBuilder;
import org.abh.common.text.TextUtils;

import edu.columbia.rdf.edb.Person;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class WebAuthentication {
	/**
	 * Change codes every 5 minutes.
	 */
	public static final long OTK_STEP_SIZE = 300000;

	private static final String SQL_DELETE_SESSION = 
			"DELETE FROM login_sessions WHERE login_sessions.person_id = ?";

	private static final String SQL_ADD_SESSION = 
			"INSERT INTO login_sessions (key, person_id) VALUES (?, ?)";

	private static final String LOGIN_IP_SQL = 
			"SELECT login_ip_address.ip_address, login_ip_address.person_id FROM login_ip_address";

	private static final String LOGIN_SQL =
			"SELECT login_persons.person_id, login_persons.password_hash_salted, login_persons.salt FROM login_persons WHERE login_persons.user_name = ?";

	private final static String SAMPLE_PERMISSIONS_SQL = 
			"SELECT sample_permissions.id FROM sample_permissions WHERE sample_permissions.sample_id = ? AND sample_permissions.person_id = ?";

	private final static String EXPERIMENT_PERMISSIONS_SQL = 
			"SELECT experiment_permissions.id FROM experiment_permissions WHERE experiment_permissions.experiment_id = ? AND experiment_permissions.person_id = ?";

	private final static String VFS_PERMISSIONS_SQL = 
			"SELECT vfs_permissions.id FROM vfs_permissions WHERE vfs_permissions.vfs_id = ? AND vfs_permissions.person_id = ?";


	private static final String PERSON_SQL = 
			"SELECT persons.id, persons.first_name, persons.last_name, persons.email FROM persons WHERE persons.id = ?";

	private static final String USER_SQL = 
			"SELECT persons.id FROM persons WHERE persons.public_uuid = ?";


	private static final String BLOCKED_IP_ADDRESS = "blocked";

	private static final String VALIDATE_IP_SQL = 
			"SELECT login_ip_address.id FROM login_ip_address WHERE login_ip_address.person_id = ? AND (login_ip_address.ip_address = '*' OR login_ip_address.ip_address LIKE ?)";

	private static final String KEY_SQL = 
			"SELECT persons.api_key FROM persons WHERE persons.id = ?";

	private static final String SQL_LOGIN_ATTEMPT = 
			"INSERT INTO login_attempts (person_id, ip_address, success) VALUES (?, ?, ?)";

	private static final String USER_TYPE_SQL =
			"SELECT persons.user_type_id FROM persons WHERE persons.id = ?";



	public static final Json JSON_AUTH_FAILED_USER = 
			new JsonAuthFailed("user-id");

	public static final Json JSON_AUTH_FAILED_OTK = 
			new JsonAuthFailed("otk");

	public static final Json JSON_AUTH_FAILED_SAMPLE = 
			new JsonAuthFailed("sample");

	public static final Json JSON_AUTH_FAILED_FIELD = 
			new JsonAuthFailed("field");

	public static final Json JSON_AUTH_FAILED_EXPERIMENT =
			new JsonAuthFailed("experiment");

	public static final Json JSON_AUTH_FAILED_PATH =
			new JsonAuthFailed("path");

	/**
	 * Validate purely by ip address and negate users having to login.
	 * 
	 * @param connection
	 * @param ipAddress
	 * @return
	 * @throws SQLException
	 * @throws ParseException 
	 */
	public static int validateIpLogin(Connection connection,
			String ipAddress) throws SQLException, ParseException {

		int userId = -1;

		if (ipAddress == null) {
			return userId;
		}

		PreparedStatement statement = connection.prepareStatement(LOGIN_IP_SQL);

		try {
			DatabaseResultsTable table = JDBCConnection.getTable(statement);

			for (int i = 0; i < table.getRowCount(); ++i) {
				String ip = table.getDataAsString(i, 0);

				if (ipAddress.startsWith(ip) || ip.equals("*")) {
					userId = table.getDataAsInt(i, 1);

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
	 * @param user
	 * @param password
	 * @return
	 * @throws SQLException
	 * @throws CryptographyException
	 * @throws ParseException 
	 */
	public static int validateLogin(Connection connection,
			String user,
			String password) throws SQLException, CryptographyException, ParseException {

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
				userId = table.getDataAsInt(0, 0);
				passwordHashSalted = table.getDataAsString(0, 1);
				salt = table.getDataAsString(0, 2);
			}
		} finally {
			statement.close();
		}

		if (userId == -1) {
			return -1;
		}

		String testPasswordHashSalted = 
				Cryptography.getSHA512Hash(password, salt);

		if (!testPasswordHashSalted.equals(passwordHashSalted)) {
			return -1;
		}

		return userId;
	}


	public static Person getPerson(Connection connection, 
			int userId) throws SQLException, ParseException {
		if (userId == -1) {
			return null;
		}

		Person person = null;

		PreparedStatement statement = connection.prepareStatement(PERSON_SQL);

		try {
			statement.setInt(1, userId);

			DatabaseResultsTable table = JDBCConnection.getTable(statement);

			if (table.getRowCount() == 1) {
				person = new Person(table.getDataAsInt(0, 0),
						table.getDataAsString(0, 1),
						table.getDataAsString(0, 2),
						table.getDataAsString(0, 3));
			}
		} finally {
			statement.close();
		}

		return person;
	}

	public static String createLoginSession(Connection connection,
			int userId) throws SQLException {
		if (userId == -1) {
			return null;
		}

		PreparedStatement statement = 
				connection.prepareStatement(SQL_DELETE_SESSION);

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

	public static boolean canViewSample(Connection connection,
			int sampleId,
			Person person) throws SQLException {
		return canViewSample(connection, sampleId, person.getId());
	}

	/**
	 * Determines whether a user can view a sample by either its experiment
	 * being viewable, or else the sample.
	 * 
	 * @param connection
	 * @param context
	 * @param experimentId
	 * @param sampleId
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static boolean canViewSample(Connection connection,
			int experimentId,
			int sampleId,
			int userId) throws SQLException {

		return canViewSample(connection,
				experimentId, 
				sampleId, 
				userId,
				getIsAdminOrSuper(connection, userId));
	}

	/**
	 * Determines whether a user can view a sample based on either the
	 * experiment id or the sample id.
	 * 
	 * @param connection
	 * @param context
	 * @param experimentId
	 * @param sampleId
	 * @param userId
	 * @param isAdmin
	 * @return
	 * @throws SQLException
	 */
	public static boolean canViewSample(Connection connection,
			int experimentId,
			int sampleId,
			int userId,
			boolean isAdmin) throws SQLException {

		return isAdmin ||
				canViewExperiment(connection, experimentId, userId) || 
				canViewSample(connection, sampleId, userId);
	}

	/**
	 * Returns true if the user can view one of the given sample ids.
	 * @param connection
	 * @param context
	 * @param sampleIds
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static boolean canViewSample(Connection connection,
			final Collection<Integer> sampleIds,
			int userId) throws SQLException {
		for (int sampleId : sampleIds) {
			if (canViewSample(connection, sampleId, userId)) {
				return true;
			}
		}

		return false;
	}

	public static boolean canViewSample(Connection connection,
			int sampleId,
			int userId) throws SQLException {

		return canViewObject(connection,
				sampleId,
				userId,
				CacheManager.getInstance().getCache("sample-view-cache"),
				SAMPLE_PERMISSIONS_SQL);
	}

	public static boolean canViewExperiment(Connection connection,
			final Collection<Integer> experimentIds,
			int userId) throws SQLException {
		for (int experimentId : experimentIds) {
			if (canViewExperiment(connection, experimentId, userId)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Returns true if the person is allowed to view
	 * a particular experiment, false otherwise.
	 * 
	 * @param connection
	 * @param experimentId
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static synchronized boolean canViewExperiment(Connection connection,
			int experimentId, 
			int userId) throws SQLException {

		return canViewObject(connection,
				experimentId,
				userId,
				CacheManager.getInstance().getCache("experiment-view-cache"),
				EXPERIMENT_PERMISSIONS_SQL);
	}

	/**
	 * Object view permission is similar across different entities such
	 * as experiments and samples so this generic method handles checking
	 * and caching whether an object can be viewed.
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
	public static boolean canViewObject(Connection connection,
			int objectId,
			int userId,
			Cache cache,
			String sql) throws SQLException {

		//if (!checkViewPermissionsEnabled(context)) {
		//	return true;
		//}

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

			Map<Integer, Boolean> viewSet = 
					(Map<Integer, Boolean>)ce.getObjectValue();

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
					
					//System.err.println("web auth " + statement);

					ret = JDBCConnection.hasRecords(statement);
				} finally {
					statement.close();
				}

				viewSet.put(objectId, ret);
			}
			
			return ret;
		}

		
	}

	public static boolean getCanViewFile(Connection connection,
			ServletContext context,
			int vfsId,
			Person person) throws SQLException {
		return getCanViewFile(connection, context, vfsId, person.getId());
	}

	/**
	 * Returns true if user can get access to sample data based on being
	 * able to view a sample or experiment this file belongs to.
	 * @param connection
	 * @param context
	 * @param vfsId
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static boolean getCanViewFile(Connection connection,
			ServletContext context,
			int vfsId,
			int userId) throws SQLException {

		/*
		return canViewObject(connection,
				context,
				vfsId,
				userId,
				CacheManager.getInstance().getCache("file-view-cache"),
				VFS_PERMISSIONS_SQL);
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
		Map<Integer, Boolean> viewSet = (Map<Integer, Boolean>)ce.getObjectValue();

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

		ids = Vfs.getExperiments(connection, vfsId);

		if (canViewExperiment(connection, ids, userId)) {
			viewSet.put(vfsId, true);

			return true;
		}
		
		// The file is not viewable as an experiment or sample so conclude
		// user cannot look at it
		
		viewSet.put(vfsId, false);

		return false;
	}


	/**
	 * Returns if the user is a global user or admin since this means they
	 * can view any samples.
	 * 
	 * @param connection
	 * @param userId
	 * @return
	 * @throws SQLException
	 * @throws IllegalArgumentException
	 * @throws IllegalStateException
	 * @throws CacheException
	 * @throws ParseException
	 */
	public static boolean getIsAdminOrSuper(Connection connection, int userId) throws SQLException {
		UserType type = getUserType(connection, userId);

		return type == UserType.ADMINISTRATOR || type == UserType.SUPERUSER;
	}

	public static boolean getIsAdministrator(Connection connection, int userId) throws SQLException {
		return getUserType(connection, userId) == UserType.ADMINISTRATOR;
	}

	/**
	 * Returns true if the user is an admin.
	 * 
	 * @param connection
	 * @param context
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static boolean getIsSuperuser(Connection connection, int userId) throws SQLException {
		return getUserType(connection, userId) == UserType.SUPERUSER;
	}

	/**
	 * Returns the user type using a cache to speed up the check.
	 * 
	 * @param connection
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static UserType getUserType(Connection connection, int userId) throws SQLException {
		Cache cache = CacheManager.getInstance().getCache("user-type-cache");


		Element ce = cache.get(userId);

		if (ce != null) {
			return (UserType)ce.getObjectValue();
		}

		UserType type = null;

		//
		// Last resort, poll the database
		//

		PreparedStatement statement = 
				connection.prepareStatement(USER_TYPE_SQL);

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
	 * Authenicate a user based on an user id, their ip address and a
	 * one time random key.
	 * 
	 * @param context
	 * @param connection
	 * @param userId
	 * @param otk
	 * @param ipAddress
	 * @param step
	 * @return
	 * @throws SQLException
	 */
	public static boolean totpAuthUser(ServletContext context,
			HttpServletRequest request,
			Connection connection, 
			int userId,
			int otk) throws SQLException {

		return totpAuthUser(context,
				request,
				connection, 
				userId,
				otk,
				(Long)context.getAttribute("totp-step"));
	}

	public static boolean totpAuthUser(ServletContext context,
			HttpServletRequest request,
			Connection connection, 
			int userId,
			int otk,
			long step) throws SQLException {

		boolean auth = strictTOTPAuthUser(context,
				request,
				connection, 
				userId,
				otk,
				step);

		if (checkAuthEnabled(context)) {
			return auth;
		} else {
			return true;
		}
	}

	public static boolean strictTOTPAuthUser(ServletContext context,
			HttpServletRequest request,
			Connection connection, 
			int userId,
			int otk) throws SQLException {
		return strictTOTPAuthUser(context,
				request,
				connection, 
				userId,
				otk,
				(Long)context.getAttribute("totp-step"));
	}

	public static boolean strictTOTPAuthUser(ServletContext context,
			HttpServletRequest request,
			Connection connection, 
			int userId,
			int otk,
			long step) throws SQLException {

		if (userId == -1) {
			return false;
		}

		// First validate the ip address
		boolean validIp = validateIPAddress(connection, 
				context, 
				userId, 
				request.getRemoteAddr());

		if (!validIp) {
			return false;
		}

		// Now check the one time key is valid

		String key = getKey(context, connection, userId);

		if (TextUtils.isNullOrEmpty(key)) {
			return false;
		}

		//return TOTP.totpAuth(key, otk, step);

		return totpAuth(userId, 
				key,
				otk, 
				step);
	}

	private static boolean totpAuth(int userId, 
			String key,
			int otk, 
			long step) {
		return totpAuth(userId, 
				key,
				otk, 
				step,
				System.currentTimeMillis(),
				0);
	}

	private static boolean totpAuth(int userId, 
			String key,
			int otk, 
			long step,
			long time,
			long epoch) {

		long counter = TOTP.getCounter(time, epoch, step);

		Cache tcCache = CacheManager.getInstance().getCache("totp-tc-cache");
		//Cache otkCache = CacheManager.getInstance().getCache("totp-otk-cache");

		Element ce = tcCache.get(userId);

		long cachedCounter = -1;

		if (ce != null) {
			cachedCounter = (Long)ce.getObjectValue();

			//System.err.println("cache " + cachedCounter + " " + counter);
		}

		//ce = otkCache.get(userId);

		//long cachedOtk = -1;

		//if (ce != null) {
		//	cachedOtk = (Long)ce.getObjectValue();
		//}

		// The client and server are in the same counter frame so we don't
		// have to validate further
		if (counter == cachedCounter) {
			return true;
		}

		boolean auth = TOTP.totpAuth(key, otk, time, epoch, step);

		if (auth) {
			tcCache.put(new Element(userId, counter));
			//otkCache.put(new Element(userId, otk));
		}

		return auth;
	}

	/**
	 * Returns true if authentication has been enabled.
	 * 
	 * @param context
	 * @return
	 * @throws SQLException
	 */
	public static boolean checkAuthEnabled(ServletContext context) throws SQLException {
		return context.getAttribute("auth-enabled") != null;
	}

	/**
	 * Returns true if view permission checking is enabled.
	 * 
	 * @param context
	 * @return
	 * @throws SQLException
	 */
	public static boolean checkViewPermissionsEnabled(ServletContext context) throws SQLException {
		return context.getAttribute("view-permissions-enabled") != null;
	}

	/**
	 * Logs that the user made an attempt to login and how
	 * successful it was.
	 * 
	 * @param connection
	 * @param userId
	 * @param success
	 * @throws SQLException
	 */
	public static void logAttempt(Connection connection, 
			int userId,
			String ipAddress,
			boolean success) throws SQLException {

		PreparedStatement statement = 
				connection.prepareStatement(SQL_LOGIN_ATTEMPT);

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
	 * @param connection
	 * @param userId
	 * @return
	 * @throws SQLException
	 */
	public static String getKey(ServletContext context,
			Connection connection, 
			int userId) throws SQLException {
		Cache cache = CacheManager.getInstance().getCache("key-cache");

		Element ce = cache.get(userId);

		if (ce != null) {
			return (String)ce.getObjectValue();
		}

		PreparedStatement statement = 
				connection.prepareStatement(KEY_SQL);

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
	 * Validate that the reported user is accessing from a valid ip
	 * address.
	 * 
	 * @param context
	 * @param connection
	 * @param key
	 * @param ipAddress
	 * @return
	 * @throws SQLException
	 */
	public static boolean validateIPAddress(Connection connection,
			ServletContext context,
			int person, 
			String ipAddress) throws SQLException {
		Cache cache = CacheManager.getInstance().getCache("ip_add_cache");

		Element ce = cache.get(person);

		if (ce != null) {
			// If the cache contains blocked, then this user is not allowed
			// to login in from this ip address

			String cachedIpAddress = (String)ce.getObjectValue();

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

		PreparedStatement statement = 
				connection.prepareStatement(VALIDATE_IP_SQL);

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

	public static boolean validateTopt(ServletContext context,
			HttpServletRequest request,
			Connection connection, 
			int person,
			String otk) throws SQLException {
		String key = getKey(context, connection, person);

		if (key == null) {
			return false;
		}

		Cache cache = CacheManager.getInstance().getCache("auth_cache");

		Element ce = cache.get(person);

		if (ce != null) {
			if (ce.getObjectValue().equals(otk)) {
				return true;
			}
		}

		// Cache miss so do a full check


		long step = (Long)context.getAttribute("topt-step");

		boolean ret = TOTP.totp256Auth(key, otk, step);

		if (ret) {
			// Only store the for 5 mins if it is valid
			cache.put(new Element(person, otk));
		}

		return ret;
	}

	/**
	 * Returns the person id given their public id (which is used for
	 * authentication purposes via the api).
	 * 
	 * @param context
	 * @param connection
	 * @param publicId
	 * @return
	 * @throws SQLException
	 */
	public static synchronized int getUserId(ServletContext context,
			Connection connection, 
			String publicId) throws SQLException {
		Cache cache = CacheManager.getInstance().getCache("user-id-cache");

		Element ce = cache.get(publicId);

		if (ce != null) {
			return (int)ce.getObjectValue();
		}

		PreparedStatement statement = 
				connection.prepareStatement(USER_SQL);

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
	
	
	public static void authError(String reason, JsonBuilder json) {
		error("auth-fail", reason, json);
	}
	
	public static void error(String error, String reason, JsonBuilder json) {
		json.startObject();
		json.add(error, reason);
		json.endObject();
	}

	public static void otkAuthError(JsonBuilder json) {
		authError("otk", json);
	}

	public static void sampleAuthError(JsonBuilder json) {
		authError("sample", json);
	}

	public static void userAuthError(JsonBuilder json) {
		authError("user", json);
	}

	public static void expAuthError(JsonBuilder json) {
		authError("experiment", json);
	}

	public static void pathAuthError(JsonBuilder json) {
		authError("path", json);
	}

	public static void fieldAuthError(JsonBuilder json) {
		authError("field", json);
	}
}
