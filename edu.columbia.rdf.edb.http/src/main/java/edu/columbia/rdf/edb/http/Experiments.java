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
        new Object[] { id },
        new RowMapper<ExperimentBean>() {
          @Override
          public ExperimentBean mapRow(ResultSet rs, int rowNum)
              throws SQLException {

            /// samples.id,
            // samples.experiment_id,
            // samples.expression_type_id,
            // samples.name,
            // samples.organism_id,
            // TO_CHAR(samples.created, 'YYYY-MM-DD')

            return new ExperimentBean(rs.getInt(1), rs.getString(2),
                rs.getString(4), rs.getString(3), rs.getString(5));
          }
        });
  }

  public static List<ExperimentBean> getExperiment(JdbcTemplate jdbcTemplate,
      String publicId) throws SQLException {
    return jdbcTemplate.query(Database.EXPERIMENT_PUBLIC_ID_SQL,
        new Object[] { publicId },
        new RowMapper<ExperimentBean>() {
          @Override
          public ExperimentBean mapRow(ResultSet rs, int rowNum)
              throws SQLException {

            /// samples.id,
            // samples.experiment_id,
            // samples.expression_type_id,
            // samples.name,
            // samples.organism_id,
            // TO_CHAR(samples.created, 'YYYY-MM-DD')

            return new ExperimentBean(rs.getInt(1), rs.getString(2),
                rs.getString(4), rs.getString(3), rs.getString(5));
          }
        });
  }

  public static List<ExperimentBean> getExperiments(JdbcTemplate jdbcTemplate)
      throws SQLException {
    return jdbcTemplate.query(Database.EXPERIMENTS_SQL,
        new RowMapper<ExperimentBean>() {
          @Override
          public ExperimentBean mapRow(ResultSet rs, int rowNum)
              throws SQLException {
            return new ExperimentBean(rs.getInt(1), rs.getString(2),
                rs.getString(4), rs.getString(3), rs.getString(5));
          }
        });
  }
}
