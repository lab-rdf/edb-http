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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.jebtk.core.collections.CollectionUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

// TODO: Auto-generated Javadoc
/**
 * The Class Groups.
 */
public class Groups {

	/** The Constant GROUP_IDS_SQL. */
	private static final String GROUP_IDS_SQL = 
			"SELECT groups_persons.group_id FROM groups_persons WHERE groups_persons.person_id = ?";

	/** Return the ids of the groups associated with a sample. */
	private static final String SAMPLE_GROUPS_SQL = 
			"SELECT groups_samples.group_id FROM groups_samples WHERE groups_samples.sample_id = ?";

	/** The Constant SAMPLE_GROUPS_COUNT_SQL. */
	private static final String SAMPLE_GROUPS_COUNT_SQL = 
			"SELECT COUNT(id) FROM groups_samples WHERE groups_samples.sample_id = ? AND groups_samples.group_id = ANY(?::int[])";

	/** The Constant SAMPLE_GROUP_COUNT_SQL. */
	private static final String SAMPLE_GROUP_COUNT_SQL = 
			"SELECT COUNT(id) FROM groups_samples WHERE groups_samples.sample_id = ? AND groups_samples.group_id = ?";

	/** The Constant GROUP_SAMPLES_SQL. */
	private static final String GROUP_SAMPLES_SQL = 
			"SELECT groups_samples.sample_id FROM groups_samples WHERE groups_samples.group_id = ANY(?::int[])";


	public static final String GROUPS_SQL = 
			"SELECT groups.id, groups.name, groups.color FROM groups ORDER BY groups.name";

	public static final String GROUP_SQL = 
			"SELECT groups.id, groups.name, groups.color FROM groups WHERE groups.id = ?";
	
	
	
	private static class GroupBeanMapper implements RowMapper<GroupBean> {
		@Override
		public GroupBean mapRow(ResultSet rs, int rowNum) throws SQLException {
			return new GroupBean(rs.getInt(1), rs.getString(2), rs.getString(3));
		}
	}
	
	public static final GroupBeanMapper GROUP_BEAN_MAPPER = 
			new GroupBeanMapper();


	/**
	 * Instantiates a new groups.
	 */
	private Groups() {
		// Do nothing
	}

	/**
	 * Get group ids for a user.
	 *
	 * @param connection the connection
	 * @param userId the user id
	 * @return the collection
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings("unchecked")
	public static Collection<Integer> userGroups(Connection connection, 
			int userId) throws SQLException {

		Cache cache = CacheManager.getInstance().getCache("user-groups-cache");

		//
		// See if the item has been cached as viewable or not.
		//

		Element ce = cache.get(userId);

		if (ce != null) {
			return (Collection<Integer>)ce.getObjectValue();
		}

		Set<Integer> ids = Database.getIdsSet(connection, GROUP_IDS_SQL, userId);

		ce = new Element(userId, ids);

		cache.put(ce);

		return ids;
	}

	@SuppressWarnings("unchecked")
	public static Collection<Integer> userGroups(JdbcTemplate connection, 
			int userId) throws SQLException {

		Cache cache = CacheManager.getInstance().getCache("user-groups-cache");

		//
		// See if the item has been cached as viewable or not.
		//

		Element ce = cache.get(userId);

		if (ce != null) {
			return (Collection<Integer>)ce.getObjectValue();
		}

		List<Integer> ids = Query.queryForIds(connection, GROUP_IDS_SQL, userId);

		ce = new Element(userId, ids);

		cache.put(ce);

		return ids;
	}

	/**
	 * Returns true if the sample is in one of the groups the user belongs
	 * to.
	 *
	 * @param connection the connection
	 * @param sampleId the sample id
	 * @param groupIds the group ids
	 * @return true, if successful
	 * @throws SQLException the SQL exception
	 */
	public static boolean sampleIsInGroups(Connection connection, 
			int sampleId, 
			Collection<Integer> groupIds) throws SQLException {
		return Database.getId(connection, SAMPLE_GROUPS_COUNT_SQL, sampleId, groupIds) > 0;
	}

	/**
	 * Sample is in group.
	 *
	 * @param connection the connection
	 * @param sampleId the sample id
	 * @param groupId the group id
	 * @return true, if successful
	 * @throws SQLException the SQL exception
	 */
	public static boolean sampleIsInGroup(Connection connection, 
			int sampleId, 
			int groupId) throws SQLException {
		return Database.getId(connection, SAMPLE_GROUP_COUNT_SQL, sampleId, groupId) > 0;
	}

	/**
	 * Return the groups associated with a sample.
	 *
	 * @param connection the connection
	 * @param sampleId the sample id
	 * @return the collection
	 * @throws SQLException the SQL exception
	 */
	public static Collection<Integer> sampleGroups(Connection connection, 
			int sampleId) throws SQLException {
		return Database.getIdsSet(connection, SAMPLE_GROUPS_SQL, sampleId);
	}



	/**
	 * Return the samples associated with a set of groups.
	 *
	 * @param connection the connection
	 * @param sampleId the sample id
	 * @param groupIds the group ids
	 * @return the collection
	 * @throws SQLException the SQL exception
	 */
	public static Collection<Integer> groupSamples(Connection connection, 
			int sampleId, 
			Collection<Integer> groupIds) throws SQLException {
		return Database.getIdsSet(connection, GROUP_SAMPLES_SQL, groupIds);
	}

	/**
	 * Returns true if a user belongs to a sample's groups (and by
	 * extension the user can thus view the sample).
	 *
	 * @param connection the connection
	 * @param userId the user id
	 * @param sampleId the sample id
	 * @return true, if successful
	 * @throws SQLException the SQL exception
	 */
	public static boolean userInSampleGroups(Connection connection, 
			int userId,
			int sampleId) throws SQLException {
		Collection<Integer> userGroupIds = 
				Groups.userGroups(connection, userId);

		Collection<Integer> sampleGroupIds = 
				Groups.sampleGroups(connection, sampleId);

		// Determine if this one of the user groups ids is in the sample group ids
		return userInSampleGroups(userGroupIds, sampleGroupIds);
	}

	/**
	 * User in sample groups.
	 *
	 * @param userGroupIds the user group ids
	 * @param sampleGroupIds the sample group ids
	 * @return true, if successful
	 */
	public static boolean userInSampleGroups(Collection<Integer> userGroupIds,
			Collection<Integer> sampleGroupIds) {
		return CollectionUtils.contains(userGroupIds, sampleGroupIds);
	}

	public static List<GroupBean> getGroups(JdbcTemplate jdbcTemplate) throws SQLException {
		return Query.query(jdbcTemplate,
				GROUPS_SQL,
				GROUP_BEAN_MAPPER);
	}

	public static List<GroupBean> getGroups(JdbcTemplate jdbcTemplate,
			Collection<Integer> gids) throws SQLException {
		return Query.query(jdbcTemplate,
				GROUP_SQL,
				gids,
				GROUP_BEAN_MAPPER);
	}
	

}
