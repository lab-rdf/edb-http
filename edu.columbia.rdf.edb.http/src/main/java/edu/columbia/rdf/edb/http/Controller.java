package edu.columbia.rdf.edb.http;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RestController;

@RestController
public abstract class Controller {
  @Autowired
  protected JdbcTemplate mJdbcTemplate;

  @Autowired
  protected ServletContext mContext;

  @Autowired
  protected HttpServletRequest mRequest;

  /*
   * @Autowired private SessionFactory mSessionFactory;
   * 
   * public void setSessionFactory(SessionFactory sessionFactory) {
   * mSessionFactory = sessionFactory; }
   * 
   * public Session getSession() { Session session = null;
   * 
   * try { session = mSessionFactory.getCurrentSession(); } catch
   * (HibernateException e) { session = mSessionFactory.openSession(); }
   * 
   * return session; }
   */

  /*
   * public <TT> List<TT> query(Class<TT> c) { Session session = getSession();
   * 
   * List<TT> ret = query(session, c);
   * 
   * session.close();
   * 
   * return ret; }
   * 
   * @SuppressWarnings("unchecked") public static <T> List<T> query(Session
   * session, Class<T> c) { if (session != null) {
   * 
   * List<T> ret = null;
   * 
   * String hql = "from " + c.getName();
   * 
   * Transaction tx = session.beginTransaction();
   * 
   * //System.err.println("hql " + hql + " " + id);
   * 
   * ret = (List<T>)session .createQuery(hql) .list();
   * 
   * // Indicate all queries complete tx.commit();
   * 
   * return ret; } else { return Collections.emptyList(); } }
   * 
   * public <T> List<T> query(int id, Class<T> c) { Session session =
   * getSession();
   * 
   * List<T> ret = query(session, id, c);
   * 
   * session.close();
   * 
   * return ret; }
   * 
   * public <T> List<T> query(int id, String column, Class<T> c) { Session
   * session = getSession();
   * 
   * List<T> ret = query(session, id, column, c);
   * 
   * session.close();
   * 
   * return ret; }
   * 
   * public static <T> List<T> query(Session session, int id, Class<T> c) {
   * return query(session, id, "id", c); }
   * 
   * @SuppressWarnings("unchecked") public static <T> List<T> query(Session
   * session, int id, String column, Class<T> c) { if (session == null) {
   * Collections.emptyList(); }
   * 
   * String hql = "from " + c.getName() + " b where b." + column + " = ?";
   * 
   * Transaction tx = session.beginTransaction();
   * 
   * //System.err.println("hql " + hql + " " + id);
   * 
   * List<T> ret = (List<T>)session .createQuery(hql) .setInteger(0, id)
   * .list();
   * 
   * // Indicate all queries complete tx.commit();
   * 
   * return ret; }
   * 
   * public static <T> List<T> query(Session session, Collection<Integer> ids,
   * Class<T> c) { return query(session, ids, ids.size(), c); }
   * 
   * public static <T> List<T> query(Session session, Collection<Integer> ids,
   * int size, Class<T> c) { List<T> ret = new ArrayList<T>(size);
   * 
   * for (int id : ids) { ret.addAll(query(session, id, c)); }
   * 
   * return ret; }
   */

}
