package edu.columbia.rdf.edb.http;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.abh.common.text.TextUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class Query {
	private Query() {
		// Do nothing
	}
	
	/**
	 * Use a collection of ids to bind to an sql statement and return the
	 * union of all rows from all queries using those ids.
	 * 
	 * @param jdbcTemplate
	 * @param sql
	 * @param ids
	 * @param rowMapper
	 * @return
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
	 * @param jdbcTemplate
	 * @param sql
	 * @param id
	 * @param rowMapper
	 * @return
	 */
	public static <T> List<T> query(JdbcTemplate jdbcTemplate, 
			String sql,
			int id,
			RowMapper<T> rowMapper) {
		return jdbcTemplate.query(sql, new Object[]{id}, rowMapper);
	}
	
	public static <T> List<T> query(JdbcTemplate jdbcTemplate, 
			String sql,
			Object id,
			RowMapper<T> rowMapper) {
		return jdbcTemplate.query(sql, new Object[]{id}, rowMapper);
	}
	
	/**
	 * Create a query that includes an arbitrary number of ids.
	 * 
	 * @param jdbcTemplate
	 * @param sql
	 * @param rowMapper
	 * @param id
	 * @param ids
	 * @return
	 */
	public static <T> List<T> query(JdbcTemplate jdbcTemplate, 
			String sql,
			RowMapper<T> rowMapper,
			Object id,
			Object... ids) {
		
		Object[] values = new Object[1 + ids.length];
		
		values[0] = id;
		
		System.arraycopy(ids, 0, values, 1, ids.length);
		
		return jdbcTemplate.query(sql, values, rowMapper);
	}
	
	public static <T> List<T> query(JdbcTemplate jdbcTemplate, 
			String sql,
			RowMapper<T> rowMapper) {
		return jdbcTemplate.query(sql, rowMapper);
	}
	
	/**
	 * A revised version of query for object that will either return the first
	 * item from a row list or null if there are no rows, without throwing
	 * an exception.
	 * 
	 * @param jdbcTemplate
	 * @param sql
	 * @param id
	 * @param rowMapper
	 * @return
	 */
	public static <T> T querySingle(JdbcTemplate jdbcTemplate, 
			String sql,
			int id,
			RowMapper<T> rowMapper) {
		List<T> ret = query(jdbcTemplate, sql, id, rowMapper);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}
	
	public static <T> T querySingle(JdbcTemplate jdbcTemplate, 
			String sql,
			Object id,
			RowMapper<T> rowMapper) {
		List<T> ret = query(jdbcTemplate, sql, id, rowMapper);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}
	
	public static <T> T querySingle(JdbcTemplate jdbcTemplate, 
			String sql,
			Collection<Integer> ids,
			RowMapper<T> rowMapper) {
		List<T> ret = query(jdbcTemplate, sql, ids, rowMapper);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}
	
	public static <T> T querySingle(JdbcTemplate jdbcTemplate, 
			String sql,
			RowMapper<T> rowMapper,
			Object id,
			Object ids) {
		List<T> ret = query(jdbcTemplate, sql, rowMapper, id, ids);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}
	
	/**
	 * 
	 * @param jdbcTemplate
	 * @param sql
	 * @param ids
	 * @param size
	 * @param rowMapper
	 * @return
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

	public static String queryForString(JdbcTemplate jdbcTemplate,
			String sql,
			int id) {
		String ret = querySingle(jdbcTemplate,
				sql,
				id,
				new RowMapper<String>() {
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				return rs.getString(1);
			}
		});
		
		// Return an empty string if the object is null
		return ret != null ? ret : TextUtils.EMPTY_STRING;
	}
	
	public static int queryForId(JdbcTemplate jdbcTemplate,
			String sql) {
		List<Integer> ret = queryForIds(jdbcTemplate, sql);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return -1;
		}
	}
	
	public static int queryForId(JdbcTemplate jdbcTemplate,
			String sql,
			int id) {
		List<Integer> ret = queryForIds(jdbcTemplate, sql, id);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return -1;
		}
	}
	
	public static int queryForId(JdbcTemplate jdbcTemplate,
			String sql,
			String id) {
		List<Integer> ret = queryForIds(jdbcTemplate, sql, id);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return -1;
		}
	}
	
	public static <T> T queryForObject(JdbcTemplate jdbcTemplate,
			String sql,
			int id,
			RowMapper<T> rowMapper) {
		return jdbcTemplate.queryForObject(sql,
				new Object[]{id},
				rowMapper);
	}

	public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate, 
			String sql) {
		return query(jdbcTemplate,
				sql,
				new RowMapper<Integer>() {
					@Override
					public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getInt(1);
					}});
	}
	
	public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate, 
			String sql, 
			int id) {
		return query(jdbcTemplate,
				sql,
				id,
				new RowMapper<Integer>() {
					@Override
					public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getInt(1);
					}});
	}
	
	public static List<Integer> queryForIds(JdbcTemplate jdbcTemplate, 
			String sql, 
			String id) {
		return query(jdbcTemplate,
				sql,
				id,
				new RowMapper<Integer>() {
					@Override
					public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getInt(1);
					}});
	}

	
}
