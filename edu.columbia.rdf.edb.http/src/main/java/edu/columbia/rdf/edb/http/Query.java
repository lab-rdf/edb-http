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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jebtk.core.collections.CollectionUtils;
import org.jebtk.core.text.TextUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

// TODO: Auto-generated Javadoc
/**
 * The Class Query.
 */
public class Query {
  

  /**
   * Instantiates a new query.
   */
  private Query() {
    // Do nothing
  }

  /**
   * Use a collection of ids to bind to an sql statement and return the union of
   * all rows from all queries using those ids.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param ids the ids
   * @param rowMapper the row mapper
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      Collection<Integer> ids,
      RowMapper<T> rowMapper) {
    return query(jdbcTemplate, sql, ids, 100, rowMapper);
  }

  /**
   * JdbcTemplate query where query has one integer parameter.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param rowMapper the row mapper
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      int id,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.query(sql, new Object[] { id }, rowMapper);
  }

  /**
   * Query.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param rowMapper the row mapper
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      Object id,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.query(sql, new Object[] { id }, rowMapper);
  }

  /**
   * Return the values of a query taking an arbitrary number of input
   * parameters.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param rowMapper the row mapper
   * @param id the id
   * @param ids the ids
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Object id,
      Object... ids) {

    return jdbcTemplate
        .query(sql, CollectionUtils.concatenate(id, ids), rowMapper);
  }

  /**
   * Query.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param rowMapper the row mapper
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.query(sql, rowMapper);
  }

  /**
   * A revised version of query for object that will either return the first
   * item from a row list or null if there are no rows, without throwing an
   * exception.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param rowMapper the row mapper
   * @return the t
   */
  public static <T> T querySingle(JdbcTemplate jdbcTemplate,
      String sql,
      int id,
      RowMapper<T> rowMapper) {
    List<T> ret = query(jdbcTemplate, sql, id, rowMapper);

    return CollectionUtils.head(ret);
  }

  /**
   * Query single.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param rowMapper the row mapper
   * @return the t
   */
  public static <T> T querySingle(JdbcTemplate jdbcTemplate,
      String sql,
      Object id,
      RowMapper<T> rowMapper) {
    List<T> ret = query(jdbcTemplate, sql, id, rowMapper);

    return CollectionUtils.head(ret);
  }

  /**
   * Query single.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param ids the ids
   * @param rowMapper the row mapper
   * @return the t
   */
  public static <T> T querySingle(JdbcTemplate jdbcTemplate,
      String sql,
      Collection<Integer> ids,
      RowMapper<T> rowMapper) {
    List<T> ret = query(jdbcTemplate, sql, ids, rowMapper);

    return CollectionUtils.head(ret);
  }

  /**
   * Query single.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param rowMapper the row mapper
   * @param id the id
   * @param ids the ids
   * @return the t
   */
  public static <T> T querySingle(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Object id,
      Object ids) {
    List<T> ret = query(jdbcTemplate, sql, rowMapper, id, ids);

    return CollectionUtils.head(ret);
  }

  /**
   * Query.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param ids the ids
   * @param size the expected size of the results list.
   * @param rowMapper the row mapper
   * @return the list
   */
  public static <T> List<T> query(JdbcTemplate jdbcTemplate,
      String sql,
      Collection<Integer> ids,
      int size,
      RowMapper<T> rowMapper) {
    List<T> ret = new ArrayList<T>(size);

    for (int id : ids) {
      ret.addAll(query(jdbcTemplate, sql, id, rowMapper));
    }

    return ret;
  }

  /**
   * Query for string.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the string
   */
  public static String queryForString(JdbcTemplate jdbcTemplate,
      String sql,
      int id) {
    String ret = querySingle(jdbcTemplate, sql, id, Database.STRING_MAPPER);

    // Return an empty string if the object is null
    return TextUtils.nonNull(ret);
  }

  /**
   * Query for id.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @return the int
   */
  public static int queryForId(JdbcTemplate jdbcTemplate, String sql) {
    List<Integer> ret = queryForIds(jdbcTemplate, sql);

    return headId(ret);
  }

  /**
   * Query for id.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the int
   */
  public static int queryForId(JdbcTemplate jdbcTemplate, String sql, int id) {
    List<Integer> ret = queryForIds(jdbcTemplate, sql, id);

    return headId(ret);
  }

  /**
   * Query for a numerical id and return either the id or -1 if the query
   * returns no results.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the int
   */
  public static int queryForId(JdbcTemplate jdbcTemplate,
      String sql,
      Object id) {
    List<Integer> ret = queryForIds(jdbcTemplate, sql, id);

    return headId(ret);
  }

  /**
   * Return the integer value of a query taking multiple parameters.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param ids the ids
   * @return the int
   */
  public static int queryForId(JdbcTemplate jdbcTemplate,
      String sql,
      Object id,
      Object... ids) {
    List<Integer> ret = queryForIds(jdbcTemplate, sql, id, ids);

    return headId(ret);
  }

  /**
   * Query for object.
   *
   * @param <T> the generic type
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param rowMapper the row mapper
   * @return the t
   */
  public static <T> T queryForObject(JdbcTemplate jdbcTemplate,
      String sql,
      int id,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.queryForObject(sql, new Object[] { id }, rowMapper);
  }

  /**
   * Return a list of ids from a query.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @return the list
   */
  public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate,
      String sql) {
    return query(jdbcTemplate, sql, Database.INT_MAPPER);
  }

  /**
   * Return a list of ids from a query taking one integer parameter.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the list
   */
  public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate,
      String sql,
      int id) {
    return query(jdbcTemplate, sql, id, Database.INT_MAPPER);
  }

  /**
   * Query for ids.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the list
   */
  public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate,
      String sql,
      Object id) {
    return query(jdbcTemplate, sql, id, Database.INT_MAPPER);
  }

  /**
   * Query returning an id list taking an arbitrary number of parameters.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @param ids the ids
   * @return the list
   */
  public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate,
      String sql,
      Object id,
      Object... ids) {
    return query(jdbcTemplate, sql, Database.INT_MAPPER, id, ids);
  }

  /**
   * Returns the head of an id list.
   * 
   * @param ret
   * @return
   */
  private static int headId(final List<Integer> ret) {
    if (ret.size() > 0) {
      return ret.get(0);
    } else {
      return -1;
    }
  }
}
