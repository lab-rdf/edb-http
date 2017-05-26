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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.abh.common.cryptography.TOTP;
import org.abh.common.text.TextUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

// TODO: Auto-generated Javadoc
/**
 * The Class WebAuth provides authentication methods based on the Spring
 * Framework.
 */
public class WebAuth {
	
	/** The Constant VALIDATE_IP_SQL. */
	public static final String VALIDATE_IP_SQL = 
			"SELECT COUNT(login_ip_address.id) FROM login_ip_address WHERE login_ip_address.person_id = ? AND (login_ip_address.ip_address = '*' OR login_ip_address.ip_address LIKE ?)";

	
	/**
	 * Validate that the reported user is accessing from a valid ip
	 * address.
	 *
	 * @param context the context
	 * @param jdbcTemplate the jdbc template
	 * @param person the person
	 * @param ipAddress the ip address
	 * @return true, if successful
	 */
	public static boolean validateIPAddress(ServletContext context,
			JdbcTemplate jdbcTemplate,
			int person, 
			String ipAddress) {
		Cache cache = CacheManager.getInstance().getCache("ip_add_cache");

		Element ce = cache.get(person);

		if (ce != null) {
			// If the cache contains blocked, then this user is not allowed
			// to login in from this ip address

			String cachedIpAddress = (String)ce.getObjectValue();

			if (cachedIpAddress.equals(WebAuthentication.BLOCKED_IP_ADDRESS)) {
				return false;
			}

			if (cachedIpAddress.equals(ipAddress)) {
				return true;
			}
		}

		// We had a cache miss or else the ip address does not match the
		// cache so we resort to a longer check


		boolean valid = false;

		int count = jdbcTemplate.queryForObject(VALIDATE_IP_SQL, 
				Integer.class, 
				new Object[]{person, ipAddress});

		if (count > 0) {
			cache.put(new Element(person, ipAddress));
		} else {
			cache.put(new Element(person, WebAuthentication.BLOCKED_IP_ADDRESS));
		}

		return valid;
	}
	
	/**
	 * Gets the user id.
	 *
	 * @param jdbcTemplate the jdbc template
	 * @param user the user
	 * @return the user id
	 */
	public static int getUserId(JdbcTemplate jdbcTemplate, String user) {
		String sql = "SELECT persons.id FROM persons WHERE persons.public_uuid = ?";
		
	    Integer v = jdbcTemplate.queryForObject(sql, Integer.class, user);
	    
	    if (v != null) {
	    	return v;
	    } else {
	    	return -1;
	    }
	}
	
	/**
	 * Authenicate a user based on an user id, their ip address and a
	 * one time random key.
	 *
	 * @param context the context
	 * @param request the request
	 * @param jdbcTemplate the jdbc template
	 * @param userId the user id
	 * @param otk the otk
	 * @return true, if successful
	 */
	public static boolean totpAuthUser(ServletContext context,
			HttpServletRequest request,
			JdbcTemplate jdbcTemplate,
			int userId,
			int otk) {

		return totpAuthUser(context,
				request,
				jdbcTemplate,
				userId,
				otk,
				(long)context.getAttribute("totp-step"));
	}

	/**
	 * Totp auth user.
	 *
	 * @param context the context
	 * @param request the request
	 * @param jdbcTemplate the jdbc template
	 * @param userId the user id
	 * @param otk the otk
	 * @param step the step
	 * @return true, if successful
	 */
	public static boolean totpAuthUser(ServletContext context,
			HttpServletRequest request,
			JdbcTemplate jdbcTemplate,
			int userId,
			int otk,
			long step) {

		if (WebAuthentication.checkAuthEnabled(context)) {
			return strictTOTPAuthUser(context,
					request,
					jdbcTemplate, 
					userId,
					otk,
					step);
		} else {
			return true;
		}
	}

	/**
	 * Strict TOTP auth user. Always authenticate.
	 *
	 * @param context the context
	 * @param request the request
	 * @param jdbcTemplate the jdbc template
	 * @param userId the user id
	 * @param otk the otk
	 * @return true, if successful
	 */
	public static boolean strictTOTPAuthUser(ServletContext context,
			HttpServletRequest request,
			JdbcTemplate jdbcTemplate,
			int userId,
			int otk) {
		return strictTOTPAuthUser(context,
				request,
				jdbcTemplate, 
				userId,
				otk,
				(long)context.getAttribute("totp-step"));
	}

	/**
	 * Strict TOTP auth user.
	 *
	 * @param context the context
	 * @param request the request
	 * @param jdbcTemplate the jdbc template
	 * @param userId the user id
	 * @param otk the otk
	 * @param step the step
	 * @return true, if successful
	 */
	public static boolean strictTOTPAuthUser(ServletContext context,
			HttpServletRequest request,
			JdbcTemplate jdbcTemplate,
			int userId,
			int otk,
			long step){

		if (userId == -1) {
			return false;
		}

		// First validate the ip address
		boolean validIp = validateIPAddress(context, 
				jdbcTemplate, 
				userId, 
				request.getRemoteAddr());

		if (!validIp) {
			return false;
		}

		// Now check the one time key is valid

		String key = getKey(context, jdbcTemplate, userId);

		if (TextUtils.isNullOrEmpty(key)) {
			return false;
		}

		//return TOTP.totpAuth(key, otk, step);

		return totpAuth(userId, 
				key,
				otk, 
				step);
	}

	/**
	 * Totp auth.
	 *
	 * @param userId the user id
	 * @param key the key
	 * @param otk the otk
	 * @param step the step
	 * @return true, if successful
	 */
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

	/**
	 * Totp auth.
	 *
	 * @param userId the user id
	 * @param key the key
	 * @param otk the otk
	 * @param step the step
	 * @param time the time
	 * @param epoch the epoch
	 * @return true, if successful
	 */
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
			cachedCounter = (long)ce.getObjectValue();

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
	 * Gets the key.
	 *
	 * @param context the context
	 * @param jdbcTemplate the jdbc template
	 * @param userId the user id
	 * @return the key
	 */
	public static String getKey(ServletContext context,
			JdbcTemplate jdbcTemplate,
			int userId) {
		Cache cache = CacheManager.getInstance().getCache("key-cache");

		Element ce = cache.get(userId);

		if (ce != null) {
			return (String)ce.getObjectValue();
		}

		String key = jdbcTemplate.queryForObject(WebAuthentication.KEY_SQL, String.class, userId);

		cache.put(new Element(userId, key));

		return key;
	}
}
