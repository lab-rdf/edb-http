package edu.columbia.rdf.edb.http;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class Persons {

  public static final String PERSONS_SQL = "SELECT persons.id, persons.first_name, persons.last_name, persons.address, persons.email FROM persons";

  private static final String PERSON_SQL = PERSONS_SQL
      + " WHERE persons.id = ?";

  public static final String ADMIN_GROUP_SQL = "SELECT COUNT(groups.id) FROM groups, groups_persons WHERE groups.id = groups_persons.group_id AND groups_persons.person_id= ? AND groups.name = 'Administrator'";

  public static final String SUPERUSER_GROUP_SQL = "SELECT COUNT(groups.id) FROM groups, groups_persons WHERE groups.id = groups_persons.group_id AND groups_persons.person_id= ? AND groups.name = 'Superuser'";

  private static String GROUPS_SQL = "SELECT groups.id, groups.name, groups.color FROM groups, groups_persons WHERE groups.id = groups_persons.group_id AND groups_persons.person_id= ?";

  private static String GROUP_IDS_SQL = "SELECT groups_persons.group_id FROM groups_persons WHERE groups_persons.person_id= ?";

  private Persons() {
    // Do nothing
  }

  public static PersonBean getPerson(JdbcTemplate jdbcTemplate, int id)
      throws SQLException {
    List<PersonBean> ret = getPersons(jdbcTemplate, id);

    if (ret.size() > 0) {
      return ret.get(0);
    } else {
      return null;
    }
  }

  public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate, int id)
      throws SQLException {
    return Query
        .query(jdbcTemplate, PERSON_SQL, id, new RowMapper<PersonBean>() {
          @Override
          public PersonBean mapRow(ResultSet rs, int rowNum)
              throws SQLException {

            return new PersonBean(rs.getInt(1), rs.getString(2),
                rs.getString(3), rs.getString(4), rs.getString(5));
          }
        });
  }

  public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate,
      Collection<Integer> ids) throws SQLException {
    return Query
        .query(jdbcTemplate, PERSON_SQL, ids, new RowMapper<PersonBean>() {
          @Override
          public PersonBean mapRow(ResultSet rs, int rowNum)
              throws SQLException {

            return new PersonBean(rs.getInt(1), rs.getString(2),
                rs.getString(3), rs.getString(4), rs.getString(5));
          }
        });
  }

  public static List<PersonBean> getPersons(JdbcTemplate jdbcTemplate)
      throws SQLException {
    return Query.query(jdbcTemplate, PERSONS_SQL, new RowMapper<PersonBean>() {
      @Override
      public PersonBean mapRow(ResultSet rs, int rowNum) throws SQLException {

        return new PersonBean(rs.getInt(1), rs.getString(2), rs.getString(3),
            rs.getString(4), rs.getString(5));
      }
    });
  }

  /**
   * Returns true if user is part of the admin group.
   * 
   * @param jdbcTemplate
   * @param pid
   * @return
   */
  public static boolean isAdmin(JdbcTemplate jdbcTemplate, int pid) {
    return Query.queryForId(jdbcTemplate, ADMIN_GROUP_SQL, pid) > 0;
  }

  /**
   * Returns true if user is part of the superuser group.
   * 
   * @param jdbcTemplate
   * @param pid
   * @return
   */
  public static boolean isSuper(JdbcTemplate jdbcTemplate, int pid) {
    return Query.queryForId(jdbcTemplate, SUPERUSER_GROUP_SQL, pid) > 0;
  }

  /**
   * Return the group ids for a person.
   * 
   * @param jdbcTemplate
   * @param pid
   * @return
   */
  public static List<Integer> groupIds(JdbcTemplate jdbcTemplate, int pid) {
    return Query.queryForIds(jdbcTemplate, GROUP_IDS_SQL, pid);
  }

  /**
   * Get the groups for a person.
   * 
   * @param jdbcTemplate
   * @param pid
   * @return
   */
  public static List<GroupBean> getGroups(JdbcTemplate jdbcTemplate, int pid) {
    return Query.query(jdbcTemplate, GROUPS_SQL, pid, Groups.GROUP_BEAN_MAPPER);
  }
}
