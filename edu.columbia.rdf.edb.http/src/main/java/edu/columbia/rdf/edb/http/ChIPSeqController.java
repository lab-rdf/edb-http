package edu.columbia.rdf.edb.http;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;


public class ChIPSeqController extends AuthController {
  /** The Constant PEAK_SQL. */
  private static final String SAMPLE_CHIPSEQ_PEAK_SQL = 
      "SELECT chip_seq_peaks.id, chip_seq_peaks.name, chip_seq_peaks.genome, chip_seq_peaks.parameters FROM chip_seq_peaks WHERE chip_seq_peaks.sample_id = ?";

  private static final String CHIPSEQ_PEAK_SQL = 
      "SELECT chip_seq_peaks.id, chip_seq_peaks.name, chip_seq_peaks.genome, chip_seq_peaks.parameters, chip_seq_peaks.json FROM chip_seq_peaks WHERE chip_seq_peaks.id = ?";

  private static final RowMapper<PeaksBean> CHIPSEQ_PEAK_BEAN_MAPPER = new RowMapper<PeaksBean> () {
    @Override
    public PeaksBean mapRow(ResultSet rs, int rowNum) throws SQLException {
      int id = rs.getInt(1);
      String name = rs.getString(2);
      String genome = rs.getString(3);
      String params = rs.getString(4);

      return new PeaksBean(id, name, genome, params);
    }
  };
  
  private static final RowMapper<PeaksBean> CHIPSEQ_PEAK_JSON_BEAN_MAPPER = new RowMapper<PeaksBean> () {
    @Override
    public PeaksBean mapRow(ResultSet rs, int rowNum) throws SQLException {
      int id = rs.getInt(1);
      String name = rs.getString(2);
      String genome = rs.getString(3);
      String params = rs.getString(4);
      String json = rs.getString(5);

      return new PeaksBean(id, name, genome, params, json);
    }
  };

  /**
   * Serve dna json.
   *
   * @param key the key
   * @param totp the totp
   * @param peakId the peak id
   * @param context the context
   * @param request the request
   * @return the string
   * @throws SQLException the SQL exception
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws ParseException the parse exception
   */
  @RequestMapping("/peaks/sample/{sid}")
  public List<PeaksBean> samplePeaks(
      @RequestParam(value = "key", defaultValue = "") String key,
      @RequestParam(value = "totp", defaultValue = "-1") int totp,
      @PathVariable final int sid) throws SQLException {

    return authenticate(mContext,
        mRequest,
        mJdbcTemplate,
        key,
        totp,
        new AuthListCallBack<PeaksBean>() {
          @Override
          public List<PeaksBean> success(ServletContext context,
              HttpServletRequest request,
              JdbcTemplate jdbcTemplate,
              AuthBean auth) throws SQLException {
            return Query.asList(jdbcTemplate,
                SAMPLE_CHIPSEQ_PEAK_SQL,
                CHIPSEQ_PEAK_BEAN_MAPPER,
                sid);
          };
        });
  }
  
  /**
   * Serve dna json.
   *
   * @param key the key
   * @param totp the totp
   * @param peakId the peak id
   * @param context the context
   * @param request the request
   * @return the string
   * @throws SQLException the SQL exception
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws ParseException the parse exception
   */
  @RequestMapping("/peaks/{id}")
  public PeaksBean peaks(
      @RequestParam(value = "key", defaultValue = "") String key,
      @RequestParam(value = "totp", defaultValue = "-1") int totp,
      @PathVariable final int id) throws SQLException {

    return authenticate(mContext,
        mRequest,
        mJdbcTemplate,
        key,
        totp,
        new AuthCallBack<PeaksBean>() {
          @Override
          public PeaksBean success(ServletContext context,
              HttpServletRequest request,
              JdbcTemplate jdbcTemplate,
              AuthBean auth) throws SQLException {
            return Query.query(jdbcTemplate,
                CHIPSEQ_PEAK_SQL,
                CHIPSEQ_PEAK_JSON_BEAN_MAPPER,
                id);
          };
        });
  }
  
  
}