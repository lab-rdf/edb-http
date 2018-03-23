package edu.columbia.rdf.edb.http;

import java.sql.SQLException;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Callback authentication function that returns a list of beans as results.
 * 
 * @author antony
 *
 * @param <T>
 */
public interface AuthListCallBack<T> {
  public List<T> success(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      AuthBean auth) throws SQLException;
}
