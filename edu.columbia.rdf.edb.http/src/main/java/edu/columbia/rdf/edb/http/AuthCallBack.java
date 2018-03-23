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

import java.sql.SQLException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.jdbc.core.JdbcTemplate;

// TODO: Auto-generated Javadoc
/**
 * AuthCallBack is used to control what happens when a database authorization is
 * succesful.
 *
 * @param <T> The type of the result object being returned.
 */
public interface AuthCallBack<T> {

  /**
   * This method is run if an authorization is successful. It should return the
   * result of a query that produces a single result.
   *
   * @param context the context.
   * @param request the request.
   * @param connection the connection.
   * @return t a result object.
   * 
   * @throws SQLException the SQL exception
   */
  public T success(ServletContext context,
      HttpServletRequest request,
      JdbcTemplate jdbcTemplate,
      AuthBean auth) throws SQLException;
}
