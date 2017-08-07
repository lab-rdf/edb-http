package edu.columbia.rdf.edb.http;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.abh.common.bioinformatics.annotation.Species;
import org.abh.common.database.ResultsSetTable;
import org.abh.common.text.TextUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import edu.columbia.rdf.edb.Experiment;
import edu.columbia.rdf.edb.Person;
import edu.columbia.rdf.edb.Sample;
import edu.columbia.rdf.edb.TypeMap;

public class Samples {
	/** The Constant ALL_SAMPLES_SQL. */
	public static final String ALL_SAMPLES_SQL =
			"SELECT samples.id, samples.experiment_id, samples.expression_type_id, samples.name, samples.organism_id, TO_CHAR(samples.created, 'YYYY-MM-DD') FROM samples";
	
	public static String SAMPLE_PERSON_IDS_SQL =
			"SELECT sample_persons.person_id FROM sample_persons WHERE sample_persons.sample_id = ?";

	public static final String SAMPLE_GEO_SQL = 
			"SELECT geo_samples.id, geo_samples.name, geo_samples.geo_series_id, geo_samples.geo_platform_id FROM geo_samples WHERE geo_samples.sample_id = ?";
	
	
	public static List<SampleBean> getSamples(final JdbcTemplate jdbcTemplate) throws SQLException {
		return jdbcTemplate.query(ALL_SAMPLES_SQL,
				new RowMapper<SampleBean>() {
			@Override
			public SampleBean mapRow(ResultSet rs, int rowNum) throws SQLException {
				int id = rs.getInt(1);

				return getSample(jdbcTemplate, id);
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
		return Database.getTable(connection, ALL_SAMPLES_SQL);
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
		if (maxCount > -1) {
			return Database.getTable(connection, Database.SAMPLES_LIMIT_SQL, ids, maxCount);
		} else {
			return Database.getTable(connection, Database.SAMPLES_SQL, ids);
		}
	}

	public static List<SampleBean> getSamples(JdbcTemplate connection, 
			Collection<Integer> ids,
			int maxCount) throws SQLException {

		List<SampleBean> ret = new ArrayList<SampleBean>(1000);

		for (int id : ids) {
			ret.addAll(getSamples(connection, id));
		}

		return ret;
	}

	

	/**
	 * Get the name of a sample.
	 * 
	 * @param connection
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static String getSampleName(final JdbcTemplate connection, 
			final int id) {
		TypeBean type = Database.getType(connection, "samples", id);

		if (type != null) {
			return type.getName();
		} else {
			return TextUtils.EMPTY_STRING;
		}
	}

	/**
	 * Return a single sample in a list.
	 * 
	 * @param connection
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public static List<SampleBean> getSamples(final JdbcTemplate jdbcTemplate, 
			final int id) throws SQLException {
		return jdbcTemplate.query(Database.SAMPLE_SQL, 
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
						Groups.sampleGroups(jdbcTemplate, id);

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

	public static SampleBean getSample(final JdbcTemplate jdbcTemplate, 
			final int id) throws SQLException {
		List<SampleBean> ret = getSamples(jdbcTemplate, id);

		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
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
		return Database.getIds(connection, Database.ALL_SAMPLE_IDS_SQL);
	}

	public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate, int sid) throws SQLException {
		List<Integer> pids = Query.queryForIds(jdbcTemplate, 
				SAMPLE_PERSON_IDS_SQL, 
				sid);
		
		return Persons.getPersons(jdbcTemplate, pids);
	}
	
	public static List<GeoBean> getGeo(final JdbcTemplate jdbcTemplate, int sid) {
		return Query.query(jdbcTemplate,
				SAMPLE_GEO_SQL,
				sid,
				new RowMapper<GeoBean>() {
			@Override
			public GeoBean mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				int seriesId = rs.getInt(3);
				int platformId = rs.getInt(4);
				
				return new GeoBean(rs.getInt(1), 
						Geo.getSeries(jdbcTemplate, seriesId), 
						rs.getString(2), 
						Geo.getPlatform(jdbcTemplate, platformId));
			}
		});
	}
}
