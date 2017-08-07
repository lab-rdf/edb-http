package edu.columbia.rdf.edb.http;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class Persons {

	public static final String PERSONS_SQL =
			"SELECT persons.id, persons.first_name, persons.last_name, persons.address, persons.email FROM persons";
	
	private static final String PERSON_SQL = 
			PERSONS_SQL + " WHERE persons.id = ?";

	private Persons() {
		// Do nothing
	}
	
	public static PersonBean getPerson(JdbcTemplate jdbcTemplate, 
			int id) throws SQLException {
		List<PersonBean> ret = getPersons(jdbcTemplate, id);
		
		if (ret.size() > 0) {
			return ret.get(0);
		} else {
			return null;
		}
	}

	public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate, 
			int id) throws SQLException {
		return Query.query(jdbcTemplate,
				PERSON_SQL, 
				id,
				new RowMapper<PersonBean>() {
			@Override
			public PersonBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				return new PersonBean(rs.getInt(1), 
						rs.getString(2),
						rs.getString(4),
						rs.getString(3),
						rs.getString(5));
			}
		});
	}
	
	public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate, 
			Collection<Integer> ids) throws SQLException {
		return Query.query(jdbcTemplate,
				PERSON_SQL, 
				ids,
				new RowMapper<PersonBean>() {
			@Override
			public PersonBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				return new PersonBean(rs.getInt(1), 
						rs.getString(2),
						rs.getString(4),
						rs.getString(3),
						rs.getString(5));
			}
		});
	}

	public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate) throws SQLException {
		return Query.query(jdbcTemplate,
				PERSONS_SQL, 
				new RowMapper<PersonBean>() {
			@Override
			public PersonBean mapRow(ResultSet rs, int rowNum) throws SQLException {

				return new PersonBean(rs.getInt(1), 
						rs.getString(2),
						rs.getString(4),
						rs.getString(3),
						rs.getString(5));
			}
		});
	}
}
