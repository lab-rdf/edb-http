package edu.columbia.rdf.edb.http;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.jebtk.core.text.TextUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth")
public class AuthController extends DBController {

  /**
   * Return whether an API key is valid. This does not indicate  who
   * owns the key, only that is is valid.
   * 
   * @param key
   * @return
   * @throws SQLException
   */
  @RequestMapping("/api_key")
  public List<AuthBean> apiKey(
      @RequestParam(value = "key", defaultValue = "") String key) {
    
    List<AuthBean> ret = new ArrayList<AuthBean>();
    
    if (WebAuth.isKey(key)) {
      ret.add(AuthBean.validKeyFormat(key));
      
      int userId = WebAuth.getUserIdFromAPIKey(mJdbcTemplate, key);

      if (userId != -1) {
        ret.add(new AuthBean(key, AuthStatus.VALID_USER));
      } else {
        ret.add(AuthBean.invalidUser(key));
      }
      
    } else {
      ret.add(AuthBean.invalidKeyFormat(key));
    }
    
    return ret;
  }
  
  public static AuthBean authenticate(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp) {
    
    if (!WebAuth.isKey(key)) {
      return AuthBean.invalidKeyFormat(key);
    }

    int userId = WebAuth.getUserIdFromAPIKey(jdbcTemplate, key);

    if (userId == -1) {
      return AuthBean.invalidUser(key);
    }

    boolean auth = WebAuth
        .totpAuthUser(context, request, jdbcTemplate, userId, totp);

    if (auth) {
      UserType userType;

      if (Persons.isAdmin(jdbcTemplate, userId)) {
        userType = UserType.ADMINISTRATOR;
      } else if (Persons.isSuper(jdbcTemplate, userId)) {
        userType = UserType.SUPERUSER;
      } else {
        userType = UserType.NORMAL;
      }

      return new AuthBean(userId, key, userType);
    } else {
      return new AuthBean(userId, key, AuthStatus.INVALID_TOTP);
    }
  }

  /**
   * Authenticate and calls an AuthCallBack if authentication is successful,
   * otherwise it returns an empty list.
   * 
   * @param context
   * @param request
   * @param jdbcTemplate
   * @param key
   * @param totp
   * @param callBack
   * @return
   * @throws SQLException
   */
  public static <T> List<T> authenticate(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp,
      AuthListCallBack<T> callBack,
      int... ids) throws SQLException {

   return authenticate(context,
        request,
        jdbcTemplate,
        key,
        totp,
        UserType.NORMAL,
        callBack,
        ids);
  }
  
  /**
   * Authenticate user to view records.
   * 
   * @param context       
   * @param request
   * @param jdbcTemplate
   * @param key
   * @param totp
   * @param minUserType       User must be at least this user type to view records.
   * @param callBack
   * @return
   * @throws SQLException
   */
  public static <T> List<T> authenticate(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp,
      UserType minUserType,
      AuthListCallBack<T> callBack,
      int... ids) throws SQLException {

    if (!authIds(ids)) {
      return Collections.emptyList();
    }
    
    AuthBean auth = authenticate(context, request, jdbcTemplate, key, totp);

    if (auth.getStatus() == AuthStatus.SUCCESS && UserType.geRank(minUserType, auth.getUserType())) {
      return callBack.success(context, request, jdbcTemplate, auth);
    } else {
      // If authentication fails, return an empty list.
      return Collections.emptyList();
    }
  }

  /**
   * Test a set of ids to be valid (> 0).
   * 
   * @param ids
   * @return
   */
  public static boolean authIds(int... ids) {
    for (int id : ids) {
      if (id < 1) {
        return false;
      }
    }
    
    return true;
  }

  public static <T> T authenticate(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp,
      AuthCallBack<T> callBack) throws SQLException {

    return authenticate(context,
        request,
        jdbcTemplate,
        key,
        totp,
        UserType.NORMAL,
        callBack);
  }
  
  /**
   * Authenticate a user and validate some of the input.
   * 
   * @param context
   * @param request
   * @param jdbcTemplate
   * @param key
   * @param totp
   * @param minUserType
   * @param callBack
   * @param ids           Optionally provide a list of ids to validate before
   *                      authenticating. This can be used to validate
   *                      sample ids etc before running a query. Ids are 
   *                      only validated for being greater than zero so that
   *                      they are potentially in the database; they are not
   *                      checked for actually being in the database.
   *                      
   * @return
   * @throws SQLException
   */
  public static <T> T authenticate(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp,
      UserType minUserType,
      AuthCallBack<T> callBack,
      int... ids) throws SQLException {

    if (!authIds(ids)) {
      return null;
    }
    
    AuthBean auth = authenticate(context, request, jdbcTemplate, key, totp);

    if (auth.getStatus() == AuthStatus.SUCCESS && 
        UserType.geRank(minUserType, auth.getUserType())) {
      return callBack.success(context, request, jdbcTemplate, auth);
    } else {
      // If authentication fails, return an empty list.
      return null;
    }
  }

  /**
   * Authorize a query that returns a string. If no result are found, an empty
   * string is returned to reduce null checks.
   * 
   * @param context
   * @param request
   * @param jdbcTemplate
   * @param key
   * @param totp
   * @param callBack
   * @return
   * @throws SQLException
   */
  public static String authString(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      String key,
      int totp,
      AuthCallBack<String> callBack) throws SQLException {

    String ret = authenticate(context,
        request,
        jdbcTemplate,
        key,
        totp,
        callBack);

    return ret != null ? ret : TextUtils.EMPTY_STRING;
  }
}
