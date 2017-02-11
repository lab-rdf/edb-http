package edu.columbia.rdf.edb.http;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.abh.common.bioinformatics.annotation.Species;
import org.abh.common.bioinformatics.annotation.Type;
import org.abh.common.database.DatabaseResultsTable;
import org.abh.common.database.JDBCConnection;

import edu.columbia.rdf.edb.Experiment;
import edu.columbia.rdf.edb.Person;
import edu.columbia.rdf.edb.Sample;
import edu.columbia.rdf.edb.TypeMap;

public class Database {
	public static final String ALL_SAMPLE_IDS_SQL = 
			"SELECT samples.id FROM samples";

	
	public static final String ALL_SAMPLES_SQL =
			"SELECT samples.id, samples.experiment_id, samples.expression_type_id, samples.name, samples.organism_id, TO_CHAR(samples.created, 'YYYY-MM-DD') FROM samples";

	public static final String SAMPLE_SQL = 
			ALL_SAMPLES_SQL + " WHERE samples.id = ?";
	
	public static final String SAMPLES_SQL = 
			ALL_SAMPLES_SQL + " WHERE samples.id = ANY(?::int[]) ORDER BY samples.name";
	
	public static final String SAMPLES_LIMIT_SQL = 
			SAMPLES_SQL + " LIMIT ?";

	public static final String SAMPLE_GEO_SQL = 
			"SELECT samples_geo.id, samples_geo.geo_series_accession, samples_geo.geo_accession, samples_geo.geo_platform, TO_CHAR(samples_geo.created, 'YYYY-MM-DD') FROM samples_geo WHERE samples_geo.sample_id = ?";

	public static final String EXPERIMENTS_SQL = 
			"SELECT experiments.id, experiments.public_id, experiments.name, experiments.description, TO_CHAR(experiments.created, 'YYYY-MM-DD') FROM experiments";

	private static final String ALIAS_SQL = 
			"SELECT DISTINCT sample_aliases.sample_id FROM sample_aliases WHERE sample_aliases.name = ? LIMIT 1";


	private Database() {
		// Do nothing
	}

	public static Array createConnArray(Connection connection, 
			Collection<Integer> ids) throws SQLException {
		return connection.createArrayOf("int", ids.toArray());
	}

	public static DatabaseResultsTable getTable(Connection connection, 
			final String sql,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		DatabaseResultsTable table = null;

		try {
			statement.setInt(1, id);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	public static DatabaseResultsTable getTable(Connection connection, 
			final String sql) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		DatabaseResultsTable table = null;

		try {
			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	public static DatabaseResultsTable getTable(Connection connection, 
			final String sql,
			Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		DatabaseResultsTable table = null;

		try {
			statement.setArray(1, createConnArray(connection, ids));

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}
	
	/**
	 * Create a table from a query with two parameters, one a list of
	 * ints and the second an int, for example a query where you want to
	 * return any records matching a list of ids, but limit the number of
	 * records.
	 * 
	 * @param connection
	 * @param sql
	 * @param ids
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static DatabaseResultsTable getTable(Connection connection, 
			final String sql,
			Collection<Integer> ids,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		DatabaseResultsTable table = null;

		try {
			statement.setArray(1, createConnArray(connection, ids));
			statement.setInt(2, id);

			table = JDBCConnection.getTable(statement);
		} finally {
			statement.close();
		}

		return table;
	}

	public static int getId(Connection connection, 
			final String sql,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		int ret = -1;

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getInt(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	public static int getId(Connection connection, 
			final String sql,
			String id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		int ret = -1;

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
	 * @param connection
	 * @param sql
	 * @param ids
	 * @return
	 * @throws SQLException
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			final Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();
		
		try {
			statement.setArray(1, createConnArray(connection, ids));

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	/**
	 * Gets a list of ids where the first parameter is a list of integers
	 * to an sql query and the second parameter is an int.
	 * 
	 * @param connection
	 * @param sql
	 * @param ids
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			final Collection<Integer> ids,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();
		
		try {
			statement.setArray(1, createConnArray(connection, ids));
			statement.setInt(2, id);

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	

	public static List<Integer> getIds(Connection connection, 
			final String sql) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();

		try {
			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	public static List<Integer> getIds(Connection connection, 
			PreparedStatement statement) throws SQLException {
		List<Integer> ret = Collections.emptyList();

		ret = JDBCConnection.getIntList(statement);

		return ret;
	}

	public static List<Integer> getIds(Connection connection, 
			final String sql,
			int id,
			int... ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();

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

		return ret;
	}
	
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			int id,
			Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();

		try {
			statement.setInt(1, id);
			statement.setArray(2, createConnArray(connection, ids));

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	public static List<Integer> getIds(Connection connection, 
			final String sql,
			String text) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<Integer> ret = Collections.emptyList();

		try {
			statement.setString(1, text + "%");

			ret = JDBCConnection.getIntList(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

	public static String getString(Connection connection, 
			final String sql,
			int id) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		String ret = null;

		try {
			statement.setInt(1, id);

			ret = JDBCConnection.getString(statement);
		} finally {
			statement.close();
		}

		return ret;
	}

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
	 * @param connection
	 * @param sql
	 * @param ids
	 * @return
	 * @throws SQLException
	 */
	public static List<String> getStrings(Connection connection, 
			final String sql,
			Collection<Integer> ids) throws SQLException {
		PreparedStatement statement = 
				connection.prepareStatement(sql);

		List<String> ret = Collections.emptyList();

		try {
			ret = getStrings(connection, statement, ids);
		} finally {
			statement.close();
		}

		return ret;
	}
	
	public static List<String> getStrings(Connection connection, 
			PreparedStatement statement,
			Collection<Integer> ids) throws SQLException {
		statement.setArray(1, createConnArray(connection, ids));

		return JDBCConnection.getStringList(statement);
	}

	public static int getSampleIdFromAlias(Connection connection, String name) throws SQLException {
		return getId(connection, ALIAS_SQL, name);
	}

	public static TypeMap getTypes(Connection connection, String type) throws SQLException {
		StringBuilder buffer = new StringBuilder("SELECT ")
				.append(type)
				.append(".id, ")
				.append(type)
				.append(".name ")
				.append("FROM ")
				.append(type);
		
		//System.err.println("types " + buffer.toString());

		DatabaseResultsTable table = getTable(connection, buffer.toString());

		TypeMap map = new TypeMap();

		if (table != null) {
			for (int i = 0; i < table.getRowCount(); ++i) {
				Type t = new Type(table.getDataAsInt(i, 0), table.getDataAsString(i, 1));
				
				map.put(t.getId(), t);
			}
		}
		
		return map;
	}

	public static DatabaseResultsTable getSamplesTable(Connection connection, 
			Collection<Integer> ids,
			int maxCount) throws SQLException {
		if (maxCount > 0) {
			return getTable(connection, SAMPLES_LIMIT_SQL, ids, maxCount);
		} else {
			return getTable(connection, SAMPLES_SQL, ids);
		}
	}
	
	public static DatabaseResultsTable getSamplesTable(Connection connection) throws SQLException {
		return getTable(connection, ALL_SAMPLES_SQL);
	}

	public static Map<Integer, Experiment> getExperiments(Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}

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

	public static TypeMap getExpressionTypes(Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}

	public static TypeMap getFieldTypes(Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}

	public static List<Integer> getAllSampleIds(Connection connection) throws SQLException {
		return getIds(connection, ALL_SAMPLE_IDS_SQL);
	}

	
}
