package edu.columbia.rdf.edb.http;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

@RestController
public abstract class Controller {
  @Autowired
  protected ServletContext mContext;

  @Autowired
  protected HttpServletRequest mRequest;
}
