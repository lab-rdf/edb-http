package edu.columbia.rdf.edb.http;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RestController;

@RestController
public abstract class DBController extends Controller {
  @Autowired
  protected JdbcTemplate mJdbcTemplate;
}
