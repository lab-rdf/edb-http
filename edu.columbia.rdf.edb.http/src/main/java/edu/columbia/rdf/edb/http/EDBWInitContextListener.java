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

import java.nio.file.Path;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.jebtk.core.io.PathUtils;

// TODO: Auto-generated Javadoc
/**
 * The listener interface for receiving EDBWInitContext events. The class that
 * is interested in processing a EDBWInitContext event implements this
 * interface, and the object created with that class is registered with a
 * component using the component's <code>addEDBWInitContextListener<code>
 * method. When the EDBWInitContext event occurs, that object's appropriate
 * method is invoked.
 *
 * @see EDBWInitContextEvent
 */
public class EDBWInitContextListener implements ServletContextListener {

  /** The Constant DATA_DIR_PARAM. */
  public static final String DATA_DIR_PARAM = "data-directory";

  /** The Constant TOTP_PARAM. */
  public static final String TOTP_PARAM = "totp-step";

  /** The Constant AUTH_PARAM. */
  public static final String AUTH_PARAM = "auth-enabled";

  /** The Constant VIEW_PARAM. */
  public static final String VIEW_PARAM = "view-permissions-enabled";

  /*
   * (non-Javadoc)
   * 
   * @see javax.servlet.ServletContextListener#contextDestroyed(javax.servlet.
   * ServletContextEvent)
   */
  @Override
  public void contextDestroyed(ServletContextEvent e) {
    // Do nothing
  }

  /*
   * (non-Javadoc)
   * 
   * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.
   * ServletContextEvent)
   */
  @Override
  public void contextInitialized(ServletContextEvent e) {
    ServletContext context = e.getServletContext();

    boolean auth = Boolean.parseBoolean(context.getInitParameter(AUTH_PARAM));

    if (auth) {
      // The app tests for this setting existing to quickly
      // determine whether to validate credentials. It does not
      // matter what value is associated with auth-enabled.
      context.setAttribute(AUTH_PARAM, true);
    }

    boolean view = Boolean.parseBoolean(context.getInitParameter(VIEW_PARAM));

    if (view) {
      context.setAttribute(VIEW_PARAM, true);
    }

    context.setAttribute(TOTP_PARAM,
        Long.parseLong(context.getInitParameter(TOTP_PARAM)));

    Path dir = PathUtils.getPath(context.getInitParameter(DATA_DIR_PARAM))
        .toAbsolutePath();

    context.setAttribute("dir", dir);
    context.setAttribute(DATA_DIR_PARAM, dir);
  }

}
