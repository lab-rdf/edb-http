package edu.columbia.rdf.edb.http;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import edu.columbia.rdf.edb.Experiment;

public class Experiments {

  private static final RowMapper<ExperimentBean> EXPERIMENT_BEAN_MAPPER = new RowMapper<ExperimentBean> () {
    @Override
    public ExperimentBean mapRow(ResultSet rs, int rowNum) throws SQLException {
      return new ExperimentBean(rs.getInt(1), rs.getString(2),
          rs.getString(4), rs.getString(3), rs.getString(5));
    }
  };
  
  private Experiments() {
    // Do nothing
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

  public static List<ExperimentBean> getExperiment(JdbcTemplate jdbcTemplate,
      int id) throws SQLException {
    return jdbcTemplate.query(Database.EXPERIMENT_SQL,
        new Object[] { id }, EXPERIMENT_BEAN_MAPPER);
  }

  public static List<ExperimentBean> getExperiment(JdbcTemplate jdbcTemplate,
      String publicId) throws SQLException {
    return jdbcTemplate.query(Database.EXPERIMENT_PUBLIC_ID_SQL,
        new Object[] { publicId }, EXPERIMENT_BEAN_MAPPER);
  }

  public static List<ExperimentBean> getExperiments(JdbcTemplate jdbcTemplate)
      throws SQLException {
    return jdbcTemplate.query(Database.EXPERIMENTS_SQL, EXPERIMENT_BEAN_MAPPER);
  }
}
