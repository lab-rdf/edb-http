package edu.columbia.rdf.edb.http;

import java.nio.file.Path;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.abh.common.file.PathUtils;


public class EDBWInitContextListener implements ServletContextListener {
	public static final String DATA_DIR_PARAM = "data-directory";
	public static final String TOTP_PARAM = "totp-step";
	public static final String AUTH_PARAM = "auth-enabled";
	public static final String VIEW_PARAM = "view-permissions-enabled";
	
	@Override
	public void contextDestroyed(ServletContextEvent e) {
		// Do nothing
	}

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
		
		Path dir = PathUtils.getPath(context.getInitParameter(DATA_DIR_PARAM));
		
		context.setAttribute("dir", dir);
		context.setAttribute(DATA_DIR_PARAM, dir);
	}

}
