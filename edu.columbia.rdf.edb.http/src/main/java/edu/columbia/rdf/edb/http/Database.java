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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.abh.common.bioinformatics.annotation.Species;
import org.abh.common.bioinformatics.annotation.Type;
import org.abh.common.database.JDBCConnection;
import org.abh.common.database.ResultsSetTable;
import org.abh.common.text.TextUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import edu.columbia.rdf.edb.Experiment;
import edu.columbia.rdf.edb.Person;
import edu.columbia.rdf.edb.Sample;
import edu.columbia.rdf.edb.TypeMap;

// TODO: Auto-generated Javadoc
/**
 * The Class Database.
 */
public class Database {

	/** The Constant ALL_SAMPLE_IDS_SQL. */
	public static final String ALL_SAMPLE_IDS_SQL = 
			"SELECT samples.id FROM samples";

	/** The Constant ALL_SAMPLES_SQL. */
	public static final String ALL_SAMPLES_SQL =
			"SELECT samples.id, samples.experiment_id, samples.expression_type_id, samples.name, samples.organism_id, TO_CHAR(samples.created, 'YYYY-MM-DD') FROM samples";

	/** The Constant SAMPLE_SQL. */
	public static final String SAMPLE_SQL = 
			ALL_SAMPLES_SQL + " WHERE samples.id = ?";

	/** The Constant SAMPLES_SQL. */
	public static final String SAMPLES_SQL = 
			ALL_SAMPLES_SQL + " WHERE samples.id = ANY(?::int[]) ORDER BY samples.name";

	/** The Constant SAMPLES_LIMIT_SQL. */
	public static final String SAMPLES_LIMIT_SQL = 
			SAMPLES_SQL + " LIMIT ?";

	/** The Constant SAMPLE_GEO_SQL. */
	public static final String SAMPLE_GEO_SQL = 
			"SELECT samples_geo.id, samples_geo.geo_series_accession, samples_geo.geo_accession, samples_geo.geo_platform, TO_CHAR(samples_geo.created, 'YYYY-MM-DD') FROM samples_geo WHERE samples_geo.sample_id = ?";

	/** The Constant EXPERIMENTS_SQL. */
	public static final String EXPERIMENTS_SQL = 
			"SELECT experiments.id, experiments.public_id, experiments.name, experiments.description, TO_CHAR(experiments.created, 'YYYY-MM-DD') FROM experiments";

	public static final String EXPERIMENT_SQL = 
			EXPERIMENTS_SQL + " WHERE experiments.id = ?";
	
	public static final String EXPERIMENT_PUBLIC_ID_SQL = 
			EXPERIMENTS_SQL + " WHERE experiments.public_id = ?";

	
	/** The Constant ALIAS_SQL. */
	private static final String ALIAS_SQL = 
			"SELECT DISTINCT sample_aliases.sample_id FROM sample_aliases WHERE sample_aliases.name = ? LIMIT 1";

	private static class IntExtractor implements ResultSetExtractor<Integer> {
		@Override
		public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
			// If there is a record, return its value, otherwise -1
			return rs.next() ? rs.getInt(1) : -1;
		}
	};

	private static class StringExtractor implements ResultSetExtractor<String> {
		@Override
		public String extractData(ResultSet rs) throws SQLException, DataAccessException {
			// If there is a record, return its value, otherwise -1
			return rs.next() ? rs.getString(1) : TextUtils.EMPTY_STRING;
		}
	};

	/**
	 * Extracts ids from a database.
	 */
	private static final ResultSetExtractor<Integer> ID_EXTRACTOR = 
			new IntExtractor();

	/**
	 * Extracts strings from a database.
	 */
	private static final ResultSetExtractor<String> STRING_EXTRACTOR = 
			new StringExtractor();


	/**
	 * Instantiates a new database.
	 */
	private Database() {
		// Do nothing
	}

	/**
	 * Creates the conn array.
	 *
	 * @param connection the connection
	 * @param ids the ids
	 * @return the array
	 * @throws SQLException the SQL exception
	 */
	public static Array createConnArray(Connection connection, 
			Collection<Integer> ids) throws SQLException {
		return connection.createArrayOf("int", ids.toArray());
	}

	/**
	 * Gets the table.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @return the table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getTable(Connection connection, 
			final String sql,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		statement.setInt(1, id);

		return JDBCConnection.resultSetTable(statement);
	}

	/**
	 * Gets the table.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @return the table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getTable(Connection connection, 
			final String sql) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		return JDBCConnection.resultSetTable(statement);
	}

	/**
	 * Create a table from a query that takes a list of integers as a
	 * parameter.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @return the table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getTable(Connection connection, 
			final String sql,
			final Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = connection.prepareStatement(sql);

		statement.setArray(1, createConnArray(connection, ids));

		return JDBCConnection.resultSetTable(statement);
	}

	/**
	 * Create a table from a query with two parameters, one a list of
	 * ints and the second an int, for example a query where you want to
	 * return any records matching a list of ids, but limit the number of
	 * records.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @param id the id
	 * @return the table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getTable(Connection connection, 
			final String sql,
			Collection<Integer> ids,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		statement.setArray(1, createConnArray(connection, ids));
		statement.setInt(2, id);

		return JDBCConnection.resultSetTable(statement);
	}

	public static int getId(Connection connection, final String sql, int id) throws SQLException {
		int ret = -1;

		PreparedStatement statement = connection.prepareStatement(sql);

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Returns the numerical id of the first column of a query, which is
	 * assumed to be an id.
	 * 
	 * @param jdbcTemplate		The jdbc connection.
	 * @param sql				The sql.
	 * @param id				The id to match on.
	 * @return					The id of the matched row or -1 if no match.
	 */
	public static int getId(JdbcTemplate jdbcTemplate, 
			final String sql, 
			int id) {
		return jdbcTemplate.query(sql, new Object[]{id}, ID_EXTRACTOR);
	}

	/**
	 * Returns the numerical id of the first column of a query, which is
	 * assumed to be an id.
	 * 
	 * @param jdbcTemplate		The jdbc connection.
	 * @param sql				The sql.
	 * @param id				The id to match on.
	 * @return					The id of the matched row or -1 if no match.
	 */
	public static int getId(JdbcTemplate jdbcTemplate, 
			final String sql, 
			final String id) {
		return jdbcTemplate.query(sql, new Object[]{id}, ID_EXTRACTOR);
	}

	/**
	 * Gets the id.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @return the id
	 * @throws SQLException the SQL exception
	 */
	public static int getId(Connection connection, 
			final String sql, 
			String id) throws SQLException {

		int ret = -1;

		PreparedStatement statement = connection.prepareStatement(sql);

		try {
			statement.setString(1, id);

			ret = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Returns the ids of records matching a set of ids.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			final Collection<Integer> ids) throws SQLException {

		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {

			Array array = createConnArray(connection, ids);

			statement.setArray(1, array);

			ret = JDBCConnection.getIntList(statement);

			array.free();
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets a list of ids where the first parameter is a list of integers
	 * to an sql query and the second parameter is an int.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @param id the id
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			final Collection<Integer> ids,
			int id) throws SQLException {

		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setArray(1, createConnArray(connection, ids));
			statement.setInt(2, id);

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets the ids.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql) throws SQLException {
		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets the ids.
	 *
	 * @param connection the connection
	 * @param statement the statement
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			PreparedStatement statement) throws SQLException {
		return JDBCConnection.getIntList(statement);
	}

	/**
	 * Return the ids from an sql query using ids as parameters to the
	 * where clause.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			int id) throws SQLException {
		List<Integer> ret = null;

		PreparedStatement statement = connection.prepareStatement(sql);

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	public static List<Integer> getIds(JdbcTemplate jdbcTemplate, 
			final String sql,
			int id) throws SQLException {
		return jdbcTemplate.query(
				sql,
				new Object[]{id},
				new RowMapper<Integer>() {
					public Integer mapRow(ResultSet rs, int rowNum) throws SQLException {
						return rs.getInt(1);
					}
				});
	}

	/**
	 * Returns the ids from a query as a set of ints.
	 *
	 * @param connection the connection
	 * @param sql 		The SQL query.
	 * @param id 		A single int parameter to the query.
	 * @return the ids set
	 * @throws SQLException the SQL exception
	 */
	public static Set<Integer> getIdsSet(Connection connection, 
			final String sql,
			int id) throws SQLException {

		Set<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getIntSet(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptySet();
		}

		return ret;
	}

	/**
	 * Gets the ids set.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @return the ids set
	 * @throws SQLException the SQL exception
	 */
	public static Set<Integer> getIdsSet(Connection connection, 
			final String sql,
			final Collection<Integer> ids) throws SQLException {

		Set<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setArray(1, createConnArray(connection, ids));

			ret = JDBCConnection.getIntSet(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptySet();
		}

		return ret;
	}

	/**
	 * Return the ids from an sql query using ids as parameters to the
	 * where clause.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @param ids the ids
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			int id,
			int... ids) throws SQLException {
		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			int c = 1;

			statement.setInt(c++, id);

			for (int i : ids) {
				statement.setInt(c++, i);
			}

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets the ids.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @param ids the ids
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			int id,
			Collection<Integer> ids) throws SQLException {
		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setInt(1, id);
			statement.setArray(2, createConnArray(connection, ids));

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets the ids.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param text the text
	 * @return the ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			String text) throws SQLException {
		List<Integer> ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setString(1, text + "%");

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Get the first id from a query where the first parameter is fixed and
	 * the second can be any from a collection of values.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @param ids the ids
	 * @return the id
	 * @throws SQLException the SQL exception
	 */
	public static int getId(Connection connection, 
			final String sql,
			int id,
			Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		int ret = -1;

		try {
			statement.setInt(1, id);
			statement.setArray(2, createConnArray(connection, ids));

			ret = JDBCConnection.getId(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Get the first id from a query where the first parameter is fixed and
	 * the second can be any from a collection of values.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id1 the id 1
	 * @param id2 the id 2
	 * @return the id
	 * @throws SQLException the SQL exception
	 */
	public static int getId(Connection connection, 
			final String sql,
			int id1,
			int id2) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		int ret = -1;

		try {
			statement.setInt(1, id1);
			statement.setInt(2, id2);

			ret = JDBCConnection.getId(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Gets the string.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @return the string
	 * @throws SQLException the SQL exception
	 */
	public static String getString(Connection connection, 
			final String sql,
			int id) throws SQLException {
		String ret = null;

		PreparedStatement statement = 
				connection.prepareStatement(sql);

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	public static String getString(JdbcTemplate jdbcTemplate, 
			final String sql,
			int id) {
		return jdbcTemplate.query(sql, new Object[]{id}, STRING_EXTRACTOR);
	}

	/**
	 * Gets the string.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param id the id
	 * @return the string
	 * @throws SQLException the SQL exception
	 */
	public static String getString(Connection connection, 
			final String sql,
			String id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		String ret = null;

		try {
			statement.setString(1, id);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	/**
	 * Returns a list of strings returned by a statement.
	 *
	 * @param connection the connection
	 * @param sql the sql
	 * @param ids the ids
	 * @return the strings
	 * @throws SQLException the SQL exception
	 */
	public static List<String> getStrings(Connection connection, 
			final String sql,
			Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<String> ret = null;

		try {
			ret = getStrings(connection, statement, ids);
		} finally {
			statement.close();
		}

		if (ret == null) {
			ret = Collections.emptyList();
		}

		return ret;
	}

	/**
	 * Gets the strings.
	 *
	 * @param connection the connection
	 * @param statement the statement
	 * @param ids the ids
	 * @return the strings
	 * @throws SQLException the SQL exception
	 */
	public static List<String> getStrings(Connection connection, 
			PreparedStatement statement,
			Collection<Integer> ids) throws SQLException {
		statement.setArray(1, createConnArray(connection, ids));

		return JDBCConnection.getStringList(statement);
	}

	/**
	 * Gets the sample id from alias.
	 *
	 * @param connection the connection
	 * @param name the name
	 * @return the sample id from alias
	 * @throws SQLException the SQL exception
	 */
	public static int getSampleIdFromAlias(Connection connection, String name) throws SQLException {
		return getId(connection, ALIAS_SQL, name);
	}

	/**
	 * Gets the types.
	 *
	 * @param connection the connection
	 * @param type the type
	 * @return the types
	 * @throws SQLException the SQL exception
	 */
	public static TypeMap getTypes(Connection connection, String type) throws SQLException {
		//System.err.println("types " + buffer.toString());

		ResultsSetTable table = getTable(connection, getTypeSql(type));

		TypeMap map = new TypeMap();

		if (table != null) {
			while (table.next()) {
				Type t = new Type(table.getInt(0), table.getString(1));

				map.put(t.getId(), t);
			}
		}

		return map;
	}

	public static List<TypeBean> getTypes(JdbcTemplate jdbcTemplate, 
			String type) throws SQLException {
		return jdbcTemplate.query(getTypeSql(type), new RowMapper<TypeBean>() {
			@Override
			public TypeBean mapRow(ResultSet rs, int rowNum) throws SQLException {
				return new TypeBean(rs.getInt(1), rs.getString(2));
			}
		});
	}

	private static final Map<String, String> TYPE_MAP =
			new HashMap<String, String>();

	public static String getTypeSql(String type) {
		if (!TYPE_MAP.containsKey(type)) {
			StringBuilder buffer = new StringBuilder("SELECT ")
					.append(type)
					.append(".id, ")
					.append(type)
					.append(".name ")
					.append("FROM ")
					.append(type)
					.append(" ORDER BY ")
					.append(type)
					.append(".name");

			TYPE_MAP.put(type, buffer.toString());
		}

		return TYPE_MAP.get(type);
	}

	/**
	 * Gets the samples table.
	 *
	 * @param connection the connection
	 * @param ids the ids
	 * @param maxCount the max count
	 * @return the samples table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getSamplesTable(Connection connection, 
			Collection<Integer> ids,
			int maxCount) throws SQLException {
		if (maxCount > 0) {
			return getTable(connection, SAMPLES_LIMIT_SQL, ids, maxCount);
		} else {
			return getTable(connection, SAMPLES_SQL, ids);
		}
	}

	public static List<SampleBean> getSamples(JdbcTemplate connection, 
			Collection<Integer> ids,
			int maxCount) throws SQLException {

		List<SampleBean> ret = new ArrayList<SampleBean>(1000);

		for (int id : ids) {
			ret.addAll(getSample(connection, id));
		}

		return ret;
	}

	public static List<SampleBean> getSample(final JdbcTemplate connection, 
			final int id) throws SQLException {
		return connection.query(SAMPLE_SQL, 
				new Object[]{id},
				new RowMapper<SampleBean>() {
			@Override
			public SampleBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				///samples.id, 
				//samples.experiment_id, 
				//samples.expression_type_id, 
				//samples.name, 
				//samples.organism_id, 
				//TO_CHAR(samples.created, 'YYYY-MM-DD')
				
				Collection<Integer> sampleGroupIds = 
						Groups.sampleGroups(connection, id);

				return new SampleBean(rs.getInt(1), 
						rs.getInt(2),
						rs.getString(4),
						rs.getInt(3),
						rs.getInt(5),
						rs.getString(6),
						sampleGroupIds);
				
				
			}
		});
	}

	/**
	 * Gets the samples table.
	 *
	 * @param connection the connection
	 * @return the samples table
	 * @throws SQLException the SQL exception
	 */
	public static ResultsSetTable getSamplesTable(Connection connection) throws SQLException {
		return getTable(connection, ALL_SAMPLES_SQL);
	}

	/**
	 * Gets the experiments.
	 *
	 * @param connection the connection
	 * @return the experiments
	 */
	public static Map<Integer, Experiment> getExperiments(Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static List<ExperimentBean> getExperiment(JdbcTemplate connection, 
			int id) throws SQLException {
		return connection.query(EXPERIMENT_SQL, 
				new Object[]{id},
				new RowMapper<ExperimentBean>() {
			@Override
			public ExperimentBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				///samples.id, 
				//samples.experiment_id, 
				//samples.expression_type_id, 
				//samples.name, 
				//samples.organism_id, 
				//TO_CHAR(samples.created, 'YYYY-MM-DD')

				return new ExperimentBean(rs.getInt(1), 
						rs.getString(2),
						rs.getString(4),
						rs.getString(3),
						rs.getString(5));
			}
		});
	}
	
	public static List<ExperimentBean> getExperiment(JdbcTemplate connection, 
			String publicId) throws SQLException {
		return connection.query(EXPERIMENT_PUBLIC_ID_SQL, 
				new Object[]{publicId},
				new RowMapper<ExperimentBean>() {
			@Override
			public ExperimentBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				///samples.id, 
				//samples.experiment_id, 
				//samples.expression_type_id, 
				//samples.name, 
				//samples.organism_id, 
				//TO_CHAR(samples.created, 'YYYY-MM-DD')

				return new ExperimentBean(rs.getInt(1), 
						rs.getString(2),
						rs.getString(4),
						rs.getString(3),
						rs.getString(5));
			}
		});
	}

	/**
	 * Gets the sample.
	 *
	 * @param connection the connection
	 * @param sampleId the sample id
	 * @param experiments the experiments
	 * @param organisms the organisms
	 * @param fieldMap the field map
	 * @param expressionTypes the expression types
	 * @param personMap the person map
	 * @return the sample
	 */
	public static Sample getSample(Connection connection, 
			int sampleId, 
			Map<Integer, Experiment> experiments,
			Map<Integer, Species> organisms, 
			TypeMap fieldMap, 
			TypeMap expressionTypes,
			Map<Integer, Person> personMap) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Gets the all sample ids.
	 *
	 * @param connection the connection
	 * @return the all sample ids
	 * @throws SQLException the SQL exception
	 */
	public static List<Integer> getAllSampleIds(Connection connection) throws SQLException {
		return getIds(connection, ALL_SAMPLE_IDS_SQL);
	}
}
