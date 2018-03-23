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
  public static <T> List<T> asList(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Collection<?> ids) {
    List<T> ret = new ArrayList<T>(100);

    for (Object id : ids) {
      ret.addAll(asList(jdbcTemplate, sql, rowMapper, id));
    }

    return ret;
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
  public static <T> List<T> asList(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Object... ids) {

    return jdbcTemplate.query(sql, ids, rowMapper);
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
  public static <T> List<T> asList(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.query(sql, rowMapper);
  }

  public static <T> T query(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper) {
    List<T> ret = asList(jdbcTemplate, sql, rowMapper);

    if (ret.size() > 0) {
      return CollectionUtils.head(ret);
    } else {
      return null;
    }
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
  public static <T> T query(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Collection<?> ids) {
    List<T> ret = asList(jdbcTemplate, sql, rowMapper, ids);

    if (ret.size() > 0) {
      return CollectionUtils.head(ret);
    } else {
      return null;
    }
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
  public static <T> T query(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Object... ids) {
    List<T> ret = asList(jdbcTemplate, sql, rowMapper, ids);

    if (ret.size() > 0) {
      return CollectionUtils.head(ret);
    } else {
      return null;
    }
  }

  public static String asString(JdbcTemplate jdbcTemplate,
      String sql) {
    String ret = query(jdbcTemplate, sql, Database.STRING_MAPPER);

    // Return an empty string if the object is null
    return TextUtils.nonNull(ret);
  }

  /**
   * Query for string.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @param id the id
   * @return the string
   */
  public static String asString(JdbcTemplate jdbcTemplate,
      String sql,
      Object... ids) {
    String ret = query(jdbcTemplate, sql, Database.STRING_MAPPER, ids);

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
  public static int asInt(JdbcTemplate jdbcTemplate, String sql) {
    List<Integer> ret = asIntList(jdbcTemplate, sql);

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
  public static int asInt(JdbcTemplate jdbcTemplate,
      String sql,
      Object... ids) {
    List<Integer> ret = asIntList(jdbcTemplate, sql, ids);

    return headId(ret);
  }
  
  public static int asInt(JdbcTemplate jdbcTemplate,
      String sql,
      Collection<?> ids) {
    List<Integer> ret = asIntList(jdbcTemplate, sql, ids);

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
  public static <T> T asObject(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper) {
    return jdbcTemplate.queryForObject(sql, rowMapper);
  }
  
  public static <T> T asObject(JdbcTemplate jdbcTemplate,
      String sql,
      RowMapper<T> rowMapper,
      Object... ids) {
    return jdbcTemplate.queryForObject(sql, ids, rowMapper);
  }

  /**
   * Return a list of ids from a query.
   *
   * @param jdbcTemplate the jdbc template
   * @param sql the sql
   * @return the list
   */
  public static List<Integer> asIntList(JdbcTemplate jdbcTemplate,
      String sql) {
    return asList(jdbcTemplate, sql, Database.INT_MAPPER);
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
  public static List<Integer> asIntList(JdbcTemplate jdbcTemplate,
      String sql,
      Object... ids) {
    return asList(jdbcTemplate, sql, Database.INT_MAPPER, ids);
  }
  
  public static List<Integer> asIntList(JdbcTemplate jdbcTemplate,
      String sql,
      Collection<?> ids) {
    return asList(jdbcTemplate, sql, Database.INT_MAPPER, ids);
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
